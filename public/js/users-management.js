// Users Management Client JS - Clean Architecture & Bulletproof Global Handlers

const UserManagementApp = {
  activeEditOriginalLimitGB: 0,
  activeEditUserId: null,

  openModal(modalId, dialogId) {
    const modal = document.getElementById(modalId);
    const dialog = document.getElementById(dialogId);
    if (!modal || !dialog) return;

    modal.classList.remove('hidden');
    modal.classList.add('flex');
    requestAnimationFrame(() => {
      dialog.classList.remove('scale-95', 'opacity-0');
      dialog.classList.add('scale-100', 'opacity-100');
    });
  },

  closeModal(modalId, dialogId) {
    const modal = document.getElementById(modalId);
    const dialog = document.getElementById(dialogId);
    if (!modal || !dialog) return;

    dialog.classList.remove('scale-100', 'opacity-100');
    dialog.classList.add('scale-95', 'opacity-0');
    setTimeout(() => {
      modal.classList.remove('flex');
      modal.classList.add('hidden');
    }, 200);
  },

  openCreateModal() {
    console.log('[UsersManagement] Opening Create User Modal');
    const form = document.getElementById('createUserForm');
    if (form) form.reset();
    
    const preview = document.getElementById('createAvatarPreview');
    const initials = document.getElementById('createAvatarInitials');
    if (preview) preview.classList.add('hidden');
    if (initials) initials.classList.remove('hidden');

    this.validateQuotaInput(50, 'create');
    this.openModal('createUserModal', 'createUserDialog');
  },

  closeCreateModal() {
    this.closeModal('createUserModal', 'createUserDialog');
  },

  openEditModal(btn) {
    if (!btn) return;
    const userId = btn.dataset.userId;
    const username = btn.dataset.username;
    const role = btn.dataset.role;
    const status = btn.dataset.status;
    const avatarPath = btn.dataset.avatarPath;
    const diskLimitBytes = parseFloat(btn.dataset.diskLimit || '0');
    const diskLimitGB = diskLimitBytes > 0 ? Math.round(diskLimitBytes / (1024 * 1024 * 1024)) : 0;

    this.activeEditUserId = userId;
    this.activeEditOriginalLimitGB = diskLimitGB;

    console.log('[UsersManagement] Opening Edit User Modal for:', username, 'Avatar:', avatarPath);

    document.getElementById('editUserId').value = userId;
    document.getElementById('editUsername').value = username;
    document.getElementById('editPassword').value = '';
    document.getElementById('editRole').value = role || 'member';
    document.getElementById('editStatus').value = status || 'active';
    document.getElementById('editDiskLimitGB').value = diskLimitGB;

    const editPreview = document.getElementById('editAvatarPreview');
    const editInitials = document.getElementById('editAvatarInitials');
    if (editPreview && editInitials) {
      if (avatarPath && avatarPath.trim() !== '') {
        editPreview.src = avatarPath;
        editPreview.classList.remove('hidden');
        editInitials.classList.add('hidden');
      } else {
        editPreview.classList.add('hidden');
        editInitials.textContent = (username || 'US').substring(0, 2).toUpperCase();
        editInitials.classList.remove('hidden');
      }
    }

    this.validateQuotaInput(diskLimitGB, 'edit', userId, diskLimitGB);
    this.openModal('editModal', 'editDialog');
  },

  closeEditModal() {
    this.closeModal('editModal', 'editDialog');
  },

  openImpersonateModal(userId, username, role) {
    console.log('[UsersManagement] Opening Impersonate Confirmation Modal for:', username);
    const modal = document.getElementById('impersonateModal');
    const form = document.getElementById('impersonateForm');
    const nameEl = document.getElementById('impersonateTargetName');
    const roleEl = document.getElementById('impersonateTargetRole');

    if (form) form.action = `/impersonate/${userId}`;
    if (nameEl) nameEl.textContent = username;
    if (roleEl) roleEl.textContent = role || 'member';

    this.openModal('impersonateModal', 'impersonateDialog');
  },

  closeImpersonateModal() {
    this.closeModal('impersonateModal', 'impersonateDialog');
  },

  previewAvatarImage(input, previewId, initialsId = null) {
    if (input.files && input.files[0]) {
      const reader = new FileReader();
      reader.onload = function(e) {
        const preview = document.getElementById(previewId);
        const initials = initialsId ? document.getElementById(initialsId) : null;
        if (preview) {
          preview.src = e.target.result;
          preview.classList.remove('hidden');
        }
        if (initials) {
          initials.classList.add('hidden');
        }
      };
      reader.readAsDataURL(input.files[0]);
    }
  },

  togglePasswordVisibility(inputId, btn) {
    const input = document.getElementById(inputId);
    if (!input) return;
    const icon = btn.querySelector('i');
    if (input.type === 'password') {
      input.type = 'text';
      if (icon) icon.className = 'ti ti-eye-off';
    } else {
      input.type = 'password';
      if (icon) icon.className = 'ti ti-eye';
    }
  },

  validateQuotaInput(val, mode, targetUserId = null, originalLimitGB = null) {
    const guideText = document.getElementById(mode === 'create' ? 'createQuotaGuideText' : 'editQuotaGuideText');
    const submitBtn = document.getElementById(mode === 'create' ? 'createUserSubmitBtn' : 'saveEditUserBtn');
    if (!guideText) return;

    const inputGB = parseFloat(val) || 0;
    const totalDiskGB = 490;
    let currentAllocatedGB = 350; // Sum across member accounts with quotas

    const origGB = originalLimitGB !== null ? originalLimitGB : this.activeEditOriginalLimitGB;

    // If editing an existing user, subtract their original quota from current allocated to calculate available room
    if (mode === 'edit' && origGB > 0) {
      currentAllocatedGB = Math.max(0, currentAllocatedGB - origGB);
    }

    const maxAvailableForUser = Math.max(0, totalDiskGB - currentAllocatedGB); // e.g. 140 GB for create, 340 GB for edit entertainment

    let isExceeded = false;

    if (inputGB <= 0) {
      guideText.className = 'text-[11px] text-amber-400 mt-1.5 font-medium flex items-center gap-1';
      guideText.innerHTML = '<i class="ti ti-alert-circle text-xs"></i> <span>Setting quota to Unlimited (No Disk Cap Applied)</span>';
    } else if (inputGB > maxAvailableForUser) {
      isExceeded = true;
      guideText.className = 'text-[11px] text-rose-400 mt-1.5 font-semibold flex items-center gap-1';
      guideText.innerHTML = `<i class="ti ti-alert-triangle text-xs"></i> <span>Warning: ${inputGB} GB exceeds Max Available Room (${maxAvailableForUser} GB Available!)</span>`;
    } else {
      const remainingAfterAlloc = maxAvailableForUser - inputGB;
      guideText.className = 'text-[11px] text-emerald-400 mt-1.5 font-medium flex items-center gap-1';
      guideText.innerHTML = `<i class="ti ti-check text-xs"></i> <span>Allocating ${inputGB} GB (Remaining Storage Pool: ${remainingAfterAlloc} GB)</span>`;
    }

    // Toggle button state to disabled / read-only when quota exceeds available room
    if (submitBtn) {
      if (isExceeded) {
        submitBtn.disabled = true;
        submitBtn.classList.add('opacity-50', 'cursor-not-allowed', 'pointer-events-none');
        submitBtn.classList.remove('hover:bg-blue-700', 'shadow-lg');
      } else {
        submitBtn.disabled = false;
        submitBtn.classList.remove('opacity-50', 'cursor-not-allowed', 'pointer-events-none');
        submitBtn.classList.add('hover:bg-blue-700', 'shadow-lg');
      }
    }
  },

  
  openUserVideosModal(userId, username) {
    console.log('[UsersManagement] Fetching videos for user:', username, 'ID:', userId);
    const usernameEl = document.getElementById('videosModalUsername');
    const container = document.getElementById('userVideosListContainer');
    if (usernameEl) usernameEl.textContent = username;
    if (container) container.innerHTML = '<div class="text-center py-8 text-gray-500 text-xs flex items-center justify-center gap-2"><i class="ti ti-loader animate-spin text-base"></i> Loading videos...</div>';

    this.openModal('userVideosModal', 'userVideosDialog');

    fetch(`/api/users/${userId}/videos`)
      .then(res => res.json())
      .then(data => {
        if (!container) return;
        if (data.success && data.videos && data.videos.length > 0) {
          container.innerHTML = data.videos.map(v => {
            const thumbPath = v.thumbnail_path || v.thumbnail;
            return `
              <div class="bg-dark-900/90 border border-dark-700/80 rounded-xl p-3 flex items-center justify-between gap-3 hover:border-dark-600 transition-colors">
                <div class="flex items-center gap-3 overflow-hidden">
                  <div class="w-12 h-12 rounded-lg bg-dark-950 border border-dark-700 overflow-hidden flex items-center justify-center text-blue-400 shrink-0">
                    ${thumbPath ? `<img src="${thumbPath}" class="w-full h-full object-cover" onerror="this.style.display='none'; this.nextElementSibling.style.display='flex';" /><div class="w-full h-full items-center justify-center hidden bg-dark-950 text-blue-400"><i class="ti ti-video text-lg"></i></div>` : `<i class="ti ti-video text-lg"></i>`}
                  </div>
                  <div class="truncate">
                    <div class="font-bold text-xs text-white truncate">${this.escapeHtml(v.title || v.original_name || 'Untitled Video')}</div>
                    <div class="text-[11px] text-gray-500 font-mono flex items-center gap-2 mt-0.5">
                      <span>${this.formatFileSize(v.file_size || 0)}</span>
                      <span>•</span>
                      <span class="capitalize text-emerald-400 font-medium">${v.status || 'ready'}</span>
                    </div>
                  </div>
                </div>
                <span class="text-[10px] font-mono bg-dark-950 px-2 py-1 rounded text-gray-400 border border-dark-700/50 shrink-0">ID: ${v.id}</span>
              </div>
            `;
          }).join('');
        } else {
          container.innerHTML = '<div class="text-center py-8 text-gray-500 text-xs">No videos uploaded by this user.</div>';
        }
      })
      .catch(err => {
        console.error('[UsersManagement] Error fetching user videos:', err);
        if (container) container.innerHTML = '<div class="text-center py-8 text-rose-400 text-xs">Failed to load videos.</div>';
      });
  },

  closeUserVideosModal() {
    this.closeModal('userVideosModal', 'userVideosDialog');
  },

  openUserStreamsModal(userId, username) {
    console.log('[UsersManagement] Fetching streams for user:', username, 'ID:', userId);
    const usernameEl = document.getElementById('streamsModalUsername');
    const container = document.getElementById('userStreamsListContainer');
    if (usernameEl) usernameEl.textContent = username;
    if (container) container.innerHTML = '<div class="text-center py-8 text-gray-500 text-xs flex items-center justify-center gap-2"><i class="ti ti-loader animate-spin text-base"></i> Loading streams...</div>';

    this.openModal('userStreamsModal', 'userStreamsDialog');

    fetch(`/api/users/${userId}/streams`)
      .then(res => res.json())
      .then(data => {
        if (!container) return;
        if (data.success && data.streams && data.streams.length > 0) {
          container.innerHTML = data.streams.map(s => {
            const thumbPath = s.youtube_thumbnail || s.video_thumbnail || s.youtube_channel_thumbnail || s.thumbnail_path || s.thumbnail;
            return `
              <div class="bg-dark-900/90 border border-dark-700/80 rounded-xl p-3 flex items-center justify-between gap-3 hover:border-dark-600 transition-colors">
                <div class="flex items-center gap-3 overflow-hidden">
                  <div class="w-16 h-10 rounded-lg bg-dark-950 border border-dark-700 overflow-hidden flex items-center justify-center ${s.status === 'live' ? 'text-emerald-400' : 'text-gray-400'} shrink-0 relative">
                    ${thumbPath ? `<img src="${thumbPath}" class="w-full h-full object-cover" onerror="this.style.display='none'; this.nextElementSibling.style.display='flex';" /><div class="w-full h-full items-center justify-center hidden bg-dark-950 text-emerald-400"><i class="ti ti-broadcast text-lg"></i></div>` : `<i class="ti ti-broadcast text-lg"></i>`}
                    ${s.status === 'live' ? '<span class="absolute top-1 right-1 w-2 h-2 rounded-full bg-emerald-500 animate-pulse"></span>' : ''}
                  </div>
                  <div class="truncate">
                    <div class="font-bold text-xs text-white truncate">${this.escapeHtml(s.title || s.name || 'Untitled Stream')}</div>
                    <div class="text-[11px] text-gray-500 font-mono flex items-center gap-2 mt-0.5">
                      <span class="${s.status === 'live' ? 'text-emerald-400 font-bold' : 'text-gray-400'} capitalize flex items-center gap-1">
                        ${s.status === 'live' ? '<i class="ti ti-point-filled text-emerald-400"></i>' : ''}${s.status || 'offline'}
                      </span>
                      <span>•</span>
                      <span>${this.escapeHtml(s.platform || 'YouTube')}</span>
                      ${s.video_title ? `<span>•</span><span class="text-gray-400 truncate max-w-[120px]">${this.escapeHtml(s.video_title)}</span>` : ''}
                    </div>
                  </div>
                </div>
                <span class="text-[10px] font-mono bg-dark-950 px-2 py-1 rounded text-gray-400 border border-dark-700/50 shrink-0">ID: ${s.id}</span>
              </div>
            `;
          }).join('');
        } else {
          container.innerHTML = '<div class="text-center py-8 text-gray-500 text-xs">No streams configured by this user.</div>';
        }
      })
      .catch(err => {
        console.error('[UsersManagement] Error fetching user streams:', err);
        if (container) container.innerHTML = '<div class="text-center py-8 text-rose-400 text-xs">Failed to load streams.</div>';
      });
  },

  closeUserStreamsModal() {
    this.closeModal('userStreamsModal', 'userStreamsDialog');
  },

  
  logSearchTimer: null,

  debounceLogSearch() {
    clearTimeout(this.logSearchTimer);
    this.logSearchTimer = setTimeout(() => this.fetchActivityLogs(), 300);
  },

  openActivityLogsModal() {
    console.log('[UsersManagement] Opening Activity Logs Modal');
    this.openModal('activityLogsModal', 'activityLogsDialog');
    this.fetchActivityLogs();
  },

  closeActivityLogsModal() {
    this.closeModal('activityLogsModal', 'activityLogsDialog');
  },

  fetchActivityLogs() {
    const container = document.getElementById('activityLogsListContainer');
    const category = document.getElementById('logCategoryFilter')?.value || 'all';
    const search = document.getElementById('logSearchInput')?.value || '';
    const countBadge = document.getElementById('logCountBadge');

    if (container) {
      container.innerHTML = '<div class="text-center py-12 text-gray-500 text-xs flex items-center justify-center gap-2"><i class="ti ti-loader animate-spin text-base text-blue-400"></i> Fetching activity logs...</div>';
    }

    const queryParams = new URLSearchParams({ category, search, limit: 100 });
    fetch(`/api/logs/activity?${queryParams.toString()}`)
      .then(res => res.json())
      .then(data => {
        if (!container) return;
        if (data.success && data.logs && data.logs.length > 0) {
          if (countBadge) countBadge.textContent = `Displaying ${data.logs.length} activity log entries`;
          container.innerHTML = data.logs.map(log => {
            const formattedDate = new Date(log.created_at).toLocaleString('id-ID', {
              day: 'numeric', month: 'short', year: 'numeric',
              hour: '2-digit', minute: '2-digit', second: '2-digit'
            });

            let categoryColor = 'bg-blue-500/10 text-blue-400 border-blue-500/20';
            let icon = 'ti-activity';
            if (log.category === 'STREAM') { categoryColor = 'bg-emerald-500/10 text-emerald-400 border-emerald-500/20'; icon = 'ti-broadcast'; }
            else if (log.category === 'GALLERY') { categoryColor = 'bg-purple-500/10 text-purple-400 border-purple-500/20'; icon = 'ti-file-video'; }
            else if (log.category === 'AUTH') { categoryColor = 'bg-amber-500/10 text-amber-400 border-amber-500/20'; icon = 'ti-shield-lock'; }
            else if (log.category === 'USER') { categoryColor = 'bg-cyan-500/10 text-cyan-400 border-cyan-500/20'; icon = 'ti-user-check'; }

            return `
              <div class="bg-dark-900/90 border border-dark-700/80 rounded-xl p-3.5 flex flex-col md:flex-row md:items-center justify-between gap-3 hover:border-dark-600 transition-all">
                <div class="flex items-start gap-3 overflow-hidden">
                  <div class="w-8 h-8 rounded-lg ${categoryColor} border flex items-center justify-center shrink-0 mt-0.5">
                    <i class="ti ${icon} text-sm"></i>
                  </div>
                  <div class="space-y-1 overflow-hidden">
                    <div class="flex items-center gap-2 flex-wrap">
                      <span class="font-bold text-xs text-white">${this.escapeHtml(log.description)}</span>
                      ${log.is_impersonated ? '<span class="text-[10px] bg-amber-500/20 text-amber-300 border border-amber-500/30 px-1.5 py-0.5 rounded font-mono" title="Action performed by Admin during impersonate"><i class="ti ti-user-check mr-0.5"></i>Impersonated by Admin</span>' : ''}
                    </div>
                    <div class="text-[11px] text-gray-400 font-mono flex items-center gap-2 flex-wrap">
                      <span>Actor: <strong class="text-blue-400">${this.escapeHtml(log.actor_username)}</strong></span>
                      ${log.target_username ? `<span>• Target: <strong class="text-gray-300">${this.escapeHtml(log.target_username)}</strong></span>` : ''}
                      <span>• IP: ${this.escapeHtml(log.ip_address || '127.0.0.1')}</span>
                    </div>
                  </div>
                </div>
                <div class="text-right shrink-0 flex md:flex-col items-center md:items-end justify-between gap-1 border-t md:border-t-0 border-dark-700/50 pt-2 md:pt-0">
                  <span class="text-[10px] font-mono px-2 py-0.5 rounded-full ${categoryColor} border uppercase font-bold">${log.category}</span>
                  <span class="text-[11px] text-gray-500 font-mono">${formattedDate} WIB</span>
                </div>
              </div>
            `;
          }).join('');
        } else {
          if (countBadge) countBadge.textContent = 'No log entries found';
          container.innerHTML = '<div class="text-center py-12 text-gray-500 text-xs">No activity log entries found matching current filters.</div>';
        }
      })
      .catch(err => {
        console.error('[UsersManagement] Error fetching logs:', err);
        if (container) container.innerHTML = '<div class="text-center py-12 text-rose-400 text-xs">Failed to load activity logs.</div>';
      });
  },

  
  exportUsersCSV() {
    console.log('[UsersManagement] Exporting Users CSV Report');
    this.showToast('Generating CSV report...', 'info');
    window.location.href = '/api/users/export-csv';
  },

  openUserActivityLogsModal(userId, username) {
    console.log(`[UsersManagement] Opening pre-filtered logs for user: ${username} (${userId})`);
    this.openModal('activityLogsModal', 'activityLogsDialog');
    
    const searchInput = document.getElementById('logSearchInput');
    if (searchInput) {
      searchInput.value = username;
    }
    this.fetchActivityLogs();
  },

  handleRevokeCurrentModalUserSessions() {
    const userId = document.getElementById('editUserId')?.value;
    const username = document.getElementById('editUsername')?.value || 'User';
    if (!userId) return;

    if (confirm(`Are you sure you want to revoke all active sessions for '${username}'? They will be forced to log in again.`)) {
      fetch('/api/users/revoke-sessions', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ userId })
      })
      .then(res => res.json())
      .then(data => {
        if (data.success) {
          this.showToast(data.message || 'Active sessions revoked', 'success');
        } else {
          this.showToast(data.message || 'Failed to revoke sessions', 'error');
        }
      })
      .catch(err => {
        console.error('[UsersManagement] Error revoking sessions:', err);
        this.showToast('Failed to revoke active sessions', 'error');
      });
    }
  },

  evaluatePasswordStrength(inputId, barId, textId) {
    const val = document.getElementById(inputId)?.value || '';
    const bar = document.getElementById(barId);
    const text = document.getElementById(textId);
    if (!bar || !text) return;

    if (val.length === 0) {
      bar.style.width = '0%';
      bar.className = 'h-full w-0 bg-rose-500 transition-all duration-300';
      text.textContent = 'Password Strength: Enter characters';
      text.className = 'text-[10px] font-mono text-gray-500 block';
    } else if (val.length < 6) {
      bar.style.width = '33%';
      bar.className = 'h-full bg-rose-500 transition-all duration-300';
      text.textContent = 'Password Strength: Weak (Min 6 chars recommended)';
      text.className = 'text-[10px] font-mono text-rose-400 font-bold block';
    } else if (val.length < 10 || !/[A-Z]/.test(val) || !/[0-9]/.test(val)) {
      bar.style.width = '66%';
      bar.className = 'h-full bg-amber-500 transition-all duration-300';
      text.textContent = 'Password Strength: Medium';
      text.className = 'text-[10px] font-mono text-amber-400 font-bold block';
    } else {
      bar.style.width = '100%';
      bar.className = 'h-full bg-emerald-500 transition-all duration-300';
      text.textContent = 'Password Strength: Strong';
      text.className = 'text-[10px] font-mono text-emerald-400 font-bold block';
    }
  },

  showToast(message, type = 'success') {
    let toastContainer = document.getElementById('customGlobalToastContainer');
    if (!toastContainer) {
      toastContainer = document.createElement('div');
      toastContainer.id = 'customGlobalToastContainer';
      toastContainer.className = 'fixed top-6 left-1/2 -translate-x-1/2 z-[99999] flex flex-col items-center gap-2.5 pointer-events-none';
      document.body.appendChild(toastContainer);
    }

    const toast = document.createElement('div');
    const isSuccess = type === 'success';
    toast.className = `pointer-events-auto flex items-center gap-3 px-5 py-3 rounded-2xl border shadow-2xl text-xs font-semibold transform transition-all duration-300 translate-y-[-10px] opacity-0 ${
      isSuccess 
        ? 'bg-dark-800/95 border-emerald-500/40 text-emerald-300 shadow-emerald-950/40' 
        : 'bg-dark-800/95 border-rose-500/40 text-rose-300 shadow-rose-950/40'
    }`;

    const icon = isSuccess ? '<i class="ti ti-circle-check text-lg text-emerald-400 shrink-0"></i>' : '<i class="ti ti-alert-circle text-lg text-rose-400 shrink-0"></i>';
    toast.innerHTML = `${icon}<span>${this.escapeHtml(message)}</span>`;
    toastContainer.appendChild(toast);

    requestAnimationFrame(() => {
      toast.classList.remove('translate-y-[-10px]', 'opacity-0');
      toast.classList.add('translate-y-0', 'opacity-100');
    });

    setTimeout(() => {
      toast.classList.remove('translate-y-0', 'opacity-100');
      toast.classList.add('translate-y-[-10px]', 'opacity-0');
      setTimeout(() => toast.remove(), 300);
    }, 3000);
  },

  escapeHtml(str) {
    if (str === null || str === undefined) return '';
    return String(str).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
  },

  formatFileSize(bytes) {
    if (bytes === 0 || !bytes) return '0 B';
    const k = 1024;
    const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
  },

  handleEditUserSubmit(e) {
    if (e && e.preventDefault) e.preventDefault();
    console.log('[UsersManagement] handleEditUserSubmit triggered!');

    const form = document.getElementById('editUserForm');
    const userId = document.getElementById('editUserId')?.value;
    const username = document.getElementById('editUsername')?.value;

    if (!userId) {
      this.showToast('Invalid User ID. Please reopen modal.', 'error');
      return false;
    }
    if (!username) {
      this.showToast('Username is required.', 'error');
      return false;
    }

    const btn = document.getElementById('saveEditUserBtn');
    const originalHTML = btn ? btn.innerHTML : 'Save Changes';
    if (btn) {
      btn.disabled = true;
      btn.innerHTML = '<i class="ti ti-loader animate-spin text-sm"></i> Saving...';
    }

    const formData = new FormData(form);

    fetch('/api/users/update', {
      method: 'POST',
      body: formData
    })
    .then(res => res.json())
    .then(data => {
      console.log('[UsersManagement] /api/users/update response:', data);
      if (btn) {
        btn.disabled = false;
        btn.innerHTML = originalHTML;
      }

      if (data.success) {
        this.closeEditModal();
        this.showToast(`Pengaturan User "${username}" berhasil disimpan!`, 'success');
        setTimeout(() => window.location.reload(), 800);
      } else {
        this.showToast(data.message || 'Error updating user', 'error');
      }
    })
    .catch(err => {
      console.error('[UsersManagement] Fetch Network Error:', err);
      this.showToast('Network error while updating user', 'error');
      if (btn) {
        btn.disabled = false;
        btn.innerHTML = originalHTML;
      }
    });

    return false;
  }
};

// Global Bridge Functions attached to window for inline onclick attributes
window.openCreateModal = () => UserManagementApp.openCreateModal();
window.closeCreateModal = () => UserManagementApp.closeCreateModal();
window.openEditUserModalFromBtn = (btn) => UserManagementApp.openEditModal(btn);
window.closeEditModal = () => UserManagementApp.closeEditModal();
window.openImpersonateModal = (userId, username, role) => UserManagementApp.openImpersonateModal(userId, username, role);
window.closeImpersonateModal = () => UserManagementApp.closeImpersonateModal();
window.previewAvatarImage = (input, previewId, initialsId) => UserManagementApp.previewAvatarImage(input, previewId, initialsId);
window.togglePasswordVisibility = (id, btn) => UserManagementApp.togglePasswordVisibility(id, btn);
window.updateQuotaAllocationGuidance = (val, mode) => UserManagementApp.validateQuotaInput(val, mode);
window.handleEditUserSubmit = (e) => UserManagementApp.handleEditUserSubmit(e);
window.showToast = (msg, type) => UserManagementApp.showToast(msg, type);

window.openUserVideosModal = (userId, username) => UserManagementApp.openUserVideosModal(userId, username);
window.closeUserVideosModal = () => UserManagementApp.closeUserVideosModal();
window.openUserStreamsModal = (userId, username) => UserManagementApp.openUserStreamsModal(userId, username);
window.closeUserStreamsModal = () => UserManagementApp.closeUserStreamsModal();

window.openActivityLogsModal = () => UserManagementApp.openActivityLogsModal();
window.closeActivityLogsModal = () => UserManagementApp.closeActivityLogsModal();
window.fetchActivityLogs = () => UserManagementApp.fetchActivityLogs();
window.debounceLogSearch = () => UserManagementApp.debounceLogSearch();

window.exportUsersCSV = () => UserManagementApp.exportUsersCSV();
window.openUserActivityLogsModal = (userId, username) => UserManagementApp.openUserActivityLogsModal(userId, username);
window.handleRevokeCurrentModalUserSessions = () => UserManagementApp.handleRevokeCurrentModalUserSessions();
window.evaluatePasswordStrength = (inputId, barId, textId) => UserManagementApp.evaluatePasswordStrength(inputId, barId, textId);
