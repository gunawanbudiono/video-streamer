const fs = require('fs');
const path = require('path');
const { spawn, execSync } = require('child_process');
const { db } = require('../db/database');
const Video = require('../models/Video');
const Stream = require('../models/Stream');

const activeOptimizations = new Map(); // videoId -> { process, progress, type }

// Get dynamic target bitrate based on resolution
function getTargetBitrate(resolutionStr) {
  if (!resolutionStr) return 4000;
  const match = resolutionStr.match(/(\d+)x(\d+)/);
  if (!match) return 4000;
  
  const height = Math.min(parseInt(match[1]), parseInt(match[2]));
  if (height >= 2160) return 18000; // 4K -> 18 Mbps
  if (height >= 1440) return 9000;  // 2K -> 9 Mbps
  if (height >= 1080) return 4000;  // 1080p -> 4 Mbps
  if (height >= 720) return 2500;   // 720p -> 2.5 Mbps
  return 1500; // SD -> 1.5 Mbps
}

// Analyze video health (Bitrate & Keyframe interval)
async function analyzeVideoHealth(videoPath) {
  try {
    if (!fs.existsSync(videoPath)) {
      return { status: 'error', error: 'File not found' };
    }

    const probeCmd = `ffprobe -v error -show_entries format=bit_rate:stream=width,height,r_frame_rate,codec_name,bit_rate -select_streams v:0 -of json "${videoPath}"`;
    const probeOutput = execSync(probeCmd, { encoding: 'utf8' });
    const probeData = JSON.parse(probeOutput);

    const stream = probeData.streams && probeData.streams[0] ? probeData.streams[0] : {};
    const format = probeData.format || {};

    const width = stream.width || 1920;
    const height = stream.height || 1080;
    const resolution = `${width}x${height}`;
    
    let bitrateKbps = Math.round((parseInt(stream.bit_rate || format.bit_rate || 0)) / 1000);
    if (!bitrateKbps || isNaN(bitrateKbps)) bitrateKbps = 390;

    const targetBitrate = getTargetBitrate(resolution);
    const isLowBitrate = bitrateKbps < (targetBitrate * 0.7);

    // Sample keyframe interval check
    let isKeyframeIssue = false;
    let maxInterval = 0;
    try {
      const gopCmd = `ffprobe -v error -select_streams v:0 -show_entries frame=pkt_pts_time,key_frame -of csv=p=0 "${videoPath}" | head -n 120`;
      const gopOutput = execSync(gopCmd, { encoding: 'utf8', timeout: 5000 });
      const lines = gopOutput.trim().split('\n');
      let lastKeyframeTime = null;

      for (const line of lines) {
        const parts = line.split(',');
        if (parts.length >= 2 && parts[1].trim() === '1') {
          const pts = parseFloat(parts[0]);
          if (lastKeyframeTime !== null) {
            const diff = pts - lastKeyframeTime;
            if (diff > maxInterval) maxInterval = diff;
          }
          lastKeyframeTime = pts;
        }
      }

      if (maxInterval > 3.5 || maxInterval === 0) {
        isKeyframeIssue = true;
      }
    } catch (e) {
      isKeyframeIssue = true;
    }

    let status = 'ok'; // ready | keyframe_issue | bitrate_and_keyframe_issue
    if (isLowBitrate && isKeyframeIssue) {
      status = 'bitrate_and_keyframe_issue';
    } else if (isLowBitrate) {
      status = 'bitrate_and_keyframe_issue';
    } else if (isKeyframeIssue) {
      status = 'keyframe_issue';
    }

    return {
      status,
      bitrateKbps,
      targetBitrate,
      resolution,
      isLowBitrate,
      isKeyframeIssue,
      keyframeInterval: maxInterval > 0 ? `${maxInterval.toFixed(1)}s` : (isKeyframeIssue ? '10.0s' : '2.0s')
    };
  } catch (err) {
    return { status: 'ok', error: err.message, bitrateKbps: 3500, targetBitrate: 4000 };
  }
}

// Check if video is currently used in an active stream
async function isVideoInActiveStream(videoId) {
  try {
    const streams = await Stream.findAll();
    return streams.some(s => s.video_id === videoId && s.status === 'active');
  } catch (e) {
    return false;
  }
}

// Perform Optimization (type: 'keyframe' | 'bitrate')
async function runOptimizationTask(videoId, type, projectRoot) {
  const video = await Video.findById(videoId);
  if (!video) throw new Error('Video not found');

  const inActive = await isVideoInActiveStream(videoId);
  if (inActive) {
    throw new Error('Video sedang aktif digunakan dalam Live Stream. Hentikan stream terlebih dahulu.');
  }

  const rawFilePath = path.join(projectRoot, 'public', video.filepath);
  if (!fs.existsSync(rawFilePath)) {
    throw new Error('File video fisik tidak ditemukan');
  }

  const health = await analyzeVideoHealth(rawFilePath);

  const targetBitrateKbps = type === 'bitrate' ? getTargetBitrate(health.resolution) : health.bitrateKbps;
  const tmpFilePath = rawFilePath.replace(/\.mp4$/i, `_tmp_${Date.now()}.mp4`);

  const args = [
    '-y',
    '-err_detect', 'ignore_err',
    '-hwaccel', 'cuda',
    '-i', rawFilePath,
    '-c:v', 'h264_nvenc',
    '-preset', 'p1',
    '-pix_fmt', 'yuv420p',
    '-g', '60',
    '-keyint_min', '30'
  ];

  if (type === 'bitrate') {
    args.push(
      '-b:v', `${targetBitrateKbps}k`,
      '-maxrate', `${Math.round(targetBitrateKbps * 1.1)}k`,
      '-bufsize', `${targetBitrateKbps * 2}k`
    );
  } else {
    args.push('-b:v', `${targetBitrateKbps}k`);
  }

  args.push(
    '-c:a', 'copy',
    '-movflags', '+faststart',
    tmpFilePath
  );

  return new Promise((resolve, reject) => {
    const proc = spawn('ffmpeg', args);
    let durationSec = video.duration || 60;

    activeOptimizations.set(videoId, {
      process: proc,
      progress: 0,
      type,
      status: 'processing',
      startTime: Date.now(),
      currentSec: 0,
      durationSec: durationSec
    });

    proc.stderr.on('data', (data) => {
      const str = data.toString();
      const timeMatches = [...str.matchAll(/time=(\d+):(\d+):(\d+(?:\.\d+)?)/g)];
      if (timeMatches.length > 0) {
        const lastMatch = timeMatches[timeMatches.length - 1];
        const hours = parseFloat(lastMatch[1]);
        const mins = parseFloat(lastMatch[2]);
        const secs = parseFloat(lastMatch[3]);
        const currentSec = hours * 3600 + mins * 60 + secs;
        
        let pct = 0;
        if (durationSec > 0) {
          pct = parseFloat(((currentSec / durationSec) * 100).toFixed(1));
          pct = Math.max(0.1, Math.min(99.9, pct));
        } else {
          pct = 1.0;
        }
        
        const currentOpt = activeOptimizations.get(videoId);
        if (currentOpt) {
          currentOpt.progress = pct;
          currentOpt.currentSec = Math.round(currentSec);
          currentOpt.durationSec = Math.round(durationSec);
        }
      }
    });

    proc.on('close', async (code) => {
      if (code === 0 && fs.existsSync(tmpFilePath)) {
        try {
          fs.unlinkSync(rawFilePath);
          fs.renameSync(tmpFilePath, rawFilePath);

          const newHealth = await analyzeVideoHealth(rawFilePath);
          await Video.update(videoId, {
            bitrate: newHealth.bitrateKbps,
            fps: '30',
            resolution: newHealth.resolution
          });

          activeOptimizations.set(videoId, { progress: 100, status: 'completed', type });
          setTimeout(() => activeOptimizations.delete(videoId), 5000);
          resolve({ success: true, message: 'Optimization completed successfully' });
        } catch (e) {
          if (fs.existsSync(tmpFilePath)) fs.unlinkSync(tmpFilePath);
          activeOptimizations.delete(videoId);
          reject(e);
        }
      } else {
        if (fs.existsSync(tmpFilePath)) fs.unlinkSync(tmpFilePath);
        activeOptimizations.delete(videoId);
        reject(new Error(`FFmpeg optimization failed with exit code ${code}`));
      }
    });

    proc.on('error', (err) => {
      if (fs.existsSync(tmpFilePath)) fs.unlinkSync(tmpFilePath);
      activeOptimizations.delete(videoId);
      reject(err);
    });
  });
}

function getOptimizationProgress(videoId) {
  return activeOptimizations.get(videoId) || null;
}

// Cancel Optimization Task
async function cancelOptimizationTask(videoId) {
  const opt = activeOptimizations.get(videoId);
  if (opt && opt.process) {
    try {
      opt.process.kill('SIGKILL');
    } catch(e){}
    activeOptimizations.delete(videoId);
    return { success: true, message: 'Optimization task cancelled' };
  }
  return { success: false, message: 'No active optimization task found' };
}

module.exports = {
  analyzeVideoHealth,
  runOptimizationTask,
  getOptimizationProgress,
  cancelOptimizationTask,
  isVideoInActiveStream
};
