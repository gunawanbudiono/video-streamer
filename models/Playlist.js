const { db } = require('../db/database');
const { v4: uuidv4 } = require('uuid');

class Playlist {
  static findAll(userId) {
    return new Promise((resolve) => {
      db.all(
        `SELECT p.*, 
         (SELECT COUNT(*) FROM playlist_videos pv 
          JOIN videos v ON pv.video_id = v.id 
          WHERE pv.playlist_id = p.id) as video_count,
         0 as audio_count,
         (SELECT GROUP_CONCAT(v2.thumbnail_path)
          FROM playlist_videos pv2
          JOIN videos v2 ON pv2.video_id = v2.id
          WHERE pv2.playlist_id = p.id) as thumbnails
         FROM playlists p 
         WHERE p.user_id = ? 
         ORDER BY p.rowid DESC`,
        [userId],
        (err, rows) => {
          if (err || !rows) {
            db.all(`SELECT * FROM playlists WHERE user_id = ?`, [userId], (err2, rows2) => {
              if (err2 || !rows2) resolve([]);
              else resolve(rows2 || []);
            });
          } else {
            resolve(rows || []);
          }
        }
      );
    });
  }

  static findById(id) {
    return new Promise((resolve, reject) => {
      db.get('SELECT * FROM playlists WHERE id = ?', [id], (err, row) => {
        if (err) {
          return reject(err);
        }
        resolve(row);
      });
    });
  }

  static findByIdWithVideos(id) {
    return new Promise((resolve, reject) => {
      db.get('SELECT * FROM playlists WHERE id = ?', [id], (err, playlist) => {
        if (err) {
          return reject(err);
        }
        if (!playlist) {
          return resolve(null);
        }

        db.all(
          `SELECT v.*, pv.position 
           FROM playlist_videos pv 
           JOIN videos v ON pv.video_id = v.id 
           WHERE pv.playlist_id = ? 
           ORDER BY pv.position ASC`,
          [id],
          (err, videos) => {
            if (err) {
              return reject(err);
            }
            const videoList = [];
            const photoList = [];
            (videos || []).forEach(item => {
              const fp = (item.filepath || '').toLowerCase();
              const isImg = item.format === 'image' || fp.includes('/photos/') || /\.(jpg|jpeg|png|webp|gif)$/i.test(fp);
              if (isImg) {
                photoList.push(item);
              } else {
                videoList.push(item);
              }
            });
            playlist.videos = videoList;
            playlist.photos = photoList;
            
            db.all(
              `SELECT v.*, pa.position 
               FROM playlist_audios pa 
               JOIN videos v ON pa.audio_id = v.id 
               WHERE pa.playlist_id = ? 
               ORDER BY pa.position ASC`,
              [id],
              (err, audios) => {
                if (err) {
                  return reject(err);
                }
                playlist.audios = audios || [];
                resolve(playlist);
              }
            );
          }
        );
      });
    });
  }

  static create(playlistData) {
    const playlistId = uuidv4();
    return new Promise((resolve, reject) => {
      db.run(
        'INSERT INTO playlists (id, name, description, is_shuffle, user_id) VALUES (?, ?, ?, ?, ?)',
        [playlistId, playlistData.name, playlistData.description || null, playlistData.is_shuffle || 0, playlistData.user_id],
        function (err) {
          if (err) {
            return reject(err);
          }
          resolve({ id: playlistId, ...playlistData });
        }
      );
    });
  }

  static update(id, playlistData) {
    const fields = [];
    const values = [];
    
    Object.entries(playlistData).forEach(([key, value]) => {
      if (key !== 'id' && key !== 'user_id') {
        fields.push(`${key} = ?`);
        values.push(value);
      }
    });
    
    fields.push('updated_at = CURRENT_TIMESTAMP');
    values.push(id);

    const query = `UPDATE playlists SET ${fields.join(', ')} WHERE id = ?`;
    
    return new Promise((resolve, reject) => {
      db.run(query, values, function (err) {
        if (err) {
          return reject(err);
        }
        resolve({ id, ...playlistData });
      });
    });
  }

  static delete(id) {
    return new Promise((resolve, reject) => {
      db.run('DELETE FROM playlists WHERE id = ?', [id], function (err) {
        if (err) {
          return reject(err);
        }
        resolve({ deleted: this.changes > 0 });
      });
    });
  }

  static addVideo(playlistId, videoId, position) {
    const id = uuidv4();
    return new Promise((resolve, reject) => {
      db.run(
        'INSERT INTO playlist_videos (id, playlist_id, video_id, position) VALUES (?, ?, ?, ?)',
        [id, playlistId, videoId, position],
        function (err) {
          if (err) {
            return reject(err);
          }
          resolve({ id, playlist_id: playlistId, video_id: videoId, position });
        }
      );
    });
  }

  static removeVideo(playlistId, videoId) {
    return new Promise((resolve, reject) => {
      db.run(
        'DELETE FROM playlist_videos WHERE playlist_id = ? AND video_id = ?',
        [playlistId, videoId],
        function (err) {
          if (err) {
            return reject(err);
          }
          resolve({ deleted: this.changes > 0 });
        }
      );
    });
  }

  static updateVideoPositions(playlistId, videoPositions) {
    return new Promise((resolve, reject) => {
      db.serialize(() => {
        db.run('BEGIN TRANSACTION');
        
        let completed = 0;
        let hasError = false;

        videoPositions.forEach(({ videoId, position }) => {
          db.run(
            'UPDATE playlist_videos SET position = ? WHERE playlist_id = ? AND video_id = ?',
            [position, playlistId, videoId],
            function (err) {
              if (err && !hasError) {
                hasError = true;
                db.run('ROLLBACK');
                return reject(err);
              }
              
              completed++;
              if (completed === videoPositions.length && !hasError) {
                db.run('COMMIT', (err) => {
                  if (err) {
                    return reject(err);
                  }
                  resolve({ updated: true });
                });
              }
            }
          );
        });
      });
    });
  }

  static getNextPosition(playlistId) {
    return new Promise((resolve, reject) => {
      db.get(
        'SELECT MAX(position) as max_position FROM playlist_videos WHERE playlist_id = ?',
        [playlistId],
        (err, row) => {
          if (err) {
            return reject(err);
          }
          resolve((row.max_position || 0) + 1);
        }
      );
    });
  }

  static addAudio(playlistId, audioId, position) {
    const id = uuidv4();
    return new Promise((resolve, reject) => {
      db.run(
        'INSERT INTO playlist_audios (id, playlist_id, audio_id, position) VALUES (?, ?, ?, ?)',
        [id, playlistId, audioId, position],
        function (err) {
          if (err) {
            return reject(err);
          }
          resolve({ id, playlist_id: playlistId, audio_id: audioId, position });
        }
      );
    });
  }

  static removeAudio(playlistId, audioId) {
    return new Promise((resolve, reject) => {
      db.run(
        'DELETE FROM playlist_audios WHERE playlist_id = ? AND audio_id = ?',
        [playlistId, audioId],
        function (err) {
          if (err) {
            return reject(err);
          }
          resolve({ deleted: this.changes > 0 });
        }
      );
    });
  }

  static clearAudios(playlistId) {
    return new Promise((resolve, reject) => {
      db.run(
        'DELETE FROM playlist_audios WHERE playlist_id = ?',
        [playlistId],
        function (err) {
          if (err) {
            return reject(err);
          }
          resolve({ deleted: this.changes });
        }
      );
    });
  }
}

module.exports = Playlist;