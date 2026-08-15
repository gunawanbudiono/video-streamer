/**
 * StreamFlow - User Management Client Controller
 * Version: 2.2.3
 * Description: Clean Code Modular Controller for Users Table, Modals, Filters, & Impersonate Feature
 */

document.addEventListener('DOMContentLoaded', () => {
  initUserManagement();
});

// Modal Controller State Manager
const ModalController = {
  open(modalId) {
    const modal = typeof modalId === 'string' ? document.querySelector(modalId) : modalId;
    if (!modal) return;
    modal.classList.remove('hidden', 'opacity-0', 'pointer-events-none');
    modal.classList.add('flex');
    const content = modal.querySelector('.transform') || modal.firstElementChild;
    if (content) {
      content.classList.remove('opacity-0', 'scale-95');
      content.classList.add('opacity-100', 'scale-100');
    }
    document.body.style.overflow = 'hidden';
  },

  close(modalId) {
    const modal = typeof modalId === 'string' ? document.querySelector(modalId) : modalId;
    if (!modal) return;
    const content = modal.querySelector('.transform') || modal.firstElementChild;
    if (content) {
      content.classList.remove('opacity-100', 'scale-100');
      content.classList.add('opacity-0', 'scale-95');
    }
    setTimeout(() => {
      modal.classList.add('hidden', 'pointer-events-none');
      modal.classList.remove('flex');
      document.body.style.overflow = 'auto';
    }, 150);
  }
};

let currentConfirmAction = null;
let activeImpersonateUserId = null;
let activeImpersonateUsername = '';

function initUserManagement() {
  setupEventListeners();
}

function setupEventListeners() {
  // Global Event Delegation for User Actions (Edit, Delete, Impersonate, Videos, Streams)
  document.addEventListener('click', (e) => {
    const btn = e.target.closest('[data-action]');
    if (!btn) return;

    const action = btn.dataset.action;
    const userId = btn.dataset.userId;
    const username = btn.dataset.username;

    if (action === 'impersonate') {
      e.preventDefault();
      e.stopPropagation();
      openImpersonateModal(userId, username);
    } else if (action === 'delete') {
      e.preventDefault();
      e.stopPropagation();
      confirmDeleteUser(userId, username);
    } else if (action === 'edit') {
      e.preventDefault();
      e.stopPropagation();
      openEditUserModal(btn);
    } else if (action === 'view-videos') {
      openVideoCollectionModal(btn);
    } else if (action === 'view-streams') {
      openStreamCollectionModal(btn);
    }
  });

  // Global Keydown Handler for Escape Key to Close Modals
  document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape') {
      closeAllModals();
    }
  });

  // Setup Search & Filter Inputs with Debounce
  const searchInput = document.getElementById('searchInput');
  const roleFilter = document.getElementById('roleFilter');
  const statusFilter = document.getElementById('statusFilter');

  if (searchInput) searchInput.addEventListener('input', debounce(filterUsersTable, 200));
  if (roleFilter) roleFilter.addEventListener('change', filterUsersTable);
  if (statusFilter) statusFilter.addEventListener('change', filterUsersTable);

  // Form Submit Handlers
  setupFormSubmissions();
}

// --- IMPERSONATE MEMBER FEATURE ---
function openImpersonateModal(userId, username) {
  activeImpersonateUserId = userId;
  activeImpersonateUsername = username;

  const targetEl = document.getElementById('impersonateTargetUsername');
  if (targetEl) {
    targetEl.innerText = `"${username}"`;
  }

  ModalController.open('#impersonateConfirmModal');
}

function closeImpersonateModal() {
  ModalController.close('#impersonateConfirmModal');
  activeImpersonateUserId = null;
  activeImpersonateUsername = '';
}

function submitImpersonate() {
  if (!activeImpersonateUserId) return;
  const btn = document.getElementById('confirmImpersonateSubmitBtn');
  const originalHTML = btn ? btn.innerHTML : 'Confirm Login';

  if (btn) {
    btn.disabled = true;
    btn.innerHTML = '<i class="ti ti-loader animate-spin text-base"></i> Logging in...';
  }

  fetch(`/api/users/${activeImpersonateUserId}/impersonate`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' }
  })
  .then(res => res.json())
  .then(data => {
    if (data.success) {
      showToast(`Logged in as ${activeImpersonateUsername}`, 'success');
      setTimeout(() => {
        window.location.href = data.redirectUrl || '/dashboard';
      }, 500);
    } else {
      showToast(data.message || 'Failed to impersonate user', 'error');
      if (btn) {
        btn.disabled = false;
        btn.innerHTML = originalHTML;
      }
      closeImpersonateModal();
    }
  })
  .catch(err => {
    console.error('Error during impersonate:', err);
    showToast('Failed to impersonate user', 'error');
    if (btn) {
      btn.disabled = false;
      btn.innerHTML = originalHTML;
    }
    closeImpersonateModal();
  });
}

// --- CONFIRMATION MODAL SYSTEM ---
function showConfirmModal(title, message, onConfirm) {
  const titleEl = document.getElementById('modalTitle');
  const messageEl = document.getElementById('modalMessage');
  if (titleEl) titleEl.textContent = title;
  if (messageEl) messageEl.textContent = message;

  currentConfirmAction = onConfirm;
  ModalController.open('#confirmModal');
}

function closeModal() {
  ModalController.close('#confirmModal');
  currentConfirmAction = null;
}

function confirmAction() {
  if (currentConfirmAction) {
    currentConfirmAction();
  }
  closeModal();
}

// --- DELETE USER ---
function confirmDeleteUser(userId, username) {
  showConfirmModal('Delete User', `Are you sure you want to delete user "${username}"? This action cannot be undone.`, () => {
    fetch('/api/users/delete', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ userId })
    })
    .then(res => res.json())
    .then(data => {
      if (data.success) {
        showToast('User deleted successfully', 'success');
        setTimeout(() => location.reload(), 800);
      } else {
        showToast('Error: ' + data.message, 'error');
      }
    })
    .catch(err => {
      console.error('Delete user error:', err);
      showToast('An error occurred while deleting user', 'error');
    });
  });
}

// --- EDIT & CREATE USER MODALS ---
function openEditUserModal(btn) {
  const userId = btn.dataset.userId;
  const username = btn.dataset.username;
  const role = btn.dataset.role;
  const status = btn.dataset.status;
  const avatar = btn.dataset.avatar;
  const diskLimit = btn.dataset.diskLimit;

  document.getElementById('editUserId').value = userId || '';
  document.getElementById('editUsername').value = username || '';
  document.getElementById('editRole').value = role || 'member';
  document.getElementById('editStatus').value = status || 'active';
  document.getElementById('editDiskLimit').value = diskLimit || 0;

  const avatarPreview = document.getElementById('editAvatarPreview');
  if (avatarPreview) {
    avatarPreview.src = avatar || '/images/default-avatar.png';
  }

  ModalController.open('#editModal');
}

function closeEditModal() {
  ModalController.close('#editModal');
}

function openCreateModal() {
  ModalController.open('#createModal');
}

function closeCreateModal() {
  ModalController.close('#createModal');
}

// --- VIDEO & STREAM COLLECTION MODALS ---
function openVideoCollectionModal(element) {
  const userId = element.dataset.userId;
  const username = element.dataset.username;

  const titleEl = document.getElementById('videoModalTitle');
  const videoList = document.getElementById('videoList');
  const emptyState = document.getElementById('videoModalEmptyState');

  if (titleEl) titleEl.textContent = `Videos by ${username}`;
  if (videoList) videoList.innerHTML = '<div class="flex items-center justify-center p-8"><div class="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-500"></div></div>';

  ModalController.open('#videoModal');

  fetch(`/api/users/${userId}/videos`)
    .then(res => res.json())
    .then(data => {
      if (data.success && data.videos && data.videos.length > 0) {
        if (emptyState) emptyState.classList.add('hidden');
        videoList.innerHTML = data.videos.map(video => `
          <div class="bg-dark-900 border border-gray-700/50 rounded-xl p-3 flex items-center space-x-3">
            <div class="w-16 h-12 rounded-lg bg-gray-800 flex-shrink-0 overflow-hidden relative">
              <img src="${video.thumbnail_path || '/images/video-placeholder.png'}" class="w-full h-full object-cover" onerror="this.src='/images/video-placeholder.png'" />
            </div>
            <div class="flex-grow min-w-0">
              <p class="text-sm font-medium text-white truncate">${escapeHtml(video.title || video.filename)}</p>
              <p class="text-xs text-gray-400">${formatFileSize(video.file_size)} • ${formatDuration(video.duration)}</p>
            </div>
          </div>
        `).join('');
      } else {
        videoList.innerHTML = '';
        if (emptyState) emptyState.classList.remove('hidden');
      }
    })
    .catch(err => {
      console.error('Fetch videos error:', err);
      videoList.innerHTML = '<p class="text-red-400 text-center p-4">Error loading videos</p>';
    });
}

function closeVideoModal() {
  ModalController.close('#videoModal');
}

function openStreamCollectionModal(element) {
  const userId = element.dataset.userId;
  const username = element.dataset.username;

  const titleEl = document.getElementById('streamModalTitle');
  const streamList = document.getElementById('streamList');
  const emptyState = document.getElementById('streamModalEmptyState');

  if (titleEl) titleEl.textContent = `Streams by ${username}`;
  if (streamList) streamList.innerHTML = '<div class="flex items-center justify-center p-8"><div class="animate-spin rounded-full h-8 w-8 border-b-2 border-red-500"></div></div>';

  ModalController.open('#streamModal');

  fetch(`/api/users/${userId}/streams`)
    .then(res => res.json())
    .then(data => {
      if (data.success && data.streams && data.streams.length > 0) {
        if (emptyState) emptyState.classList.add('hidden');
        streamList.innerHTML = data.streams.map(stream => `
          <div class="bg-dark-900 border border-gray-700/50 rounded-xl p-3 flex items-center justify-between">
            <div class="flex items-center space-x-3 min-w-0">
              <div class="w-10 h-10 rounded-lg bg-red-500/10 text-red-400 flex items-center justify-center shrink-0">
                <i class="ti ti-broadcast text-xl"></i>
              </div>
              <div class="min-w-0">
                <p class="text-sm font-medium text-white truncate">${escapeHtml(stream.title)}</p>
                <p class="text-xs text-gray-400">${stream.platform || 'YouTube'}</p>
              </div>
            </div>
            <span class="px-2.5 py-1 text-xs font-semibold rounded-md ${stream.status === 'live' ? 'bg-red-500/20 text-red-400' : 'bg-gray-700 text-gray-300'}">
              ${stream.status === 'live' ? 'LIVE' : 'Offline'}
            </span>
          </div>
        `).join('');
      } else {
        streamList.innerHTML = '';
        if (emptyState) emptyState.classList.remove('hidden');
      }
    })
    .catch(err => {
      console.error('Fetch streams error:', err);
      streamList.innerHTML = '<p class="text-red-400 text-center p-4">Error loading streams</p>';
    });
}

function closeStreamModal() {
  ModalController.close('#streamModal');
}

function closeAllModals() {
  ModalController.close('#editModal');
  ModalController.close('#createModal');
  ModalController.close('#confirmModal');
  ModalController.close('#videoModal');
  ModalController.close('#streamModal');
  ModalController.close('#impersonateConfirmModal');
}

// --- FORM SUBMIT HANDLERS ---
function setupFormSubmissions() {
  const editForm = document.getElementById('editUserForm');
  if (editForm) {
    editForm.addEventListener('submit', (e) => {
      e.preventDefault();
      const formData = new FormData(editForm);
      fetch('/api/users/update', { method: 'POST', body: formData })
        .then(res => res.json())
        .then(data => {
          if (data.success) {
            showToast('User updated successfully', 'success');
            setTimeout(() => location.reload(), 800);
          } else {
            showToast(data.message || 'Error updating user', 'error');
          }
        });
    });
  }

  const createForm = document.getElementById('createUserForm');
  if (createForm) {
    createForm.addEventListener('submit', (e) => {
      e.preventDefault();
      const formData = new FormData(createForm);
      fetch('/api/users/create', { method: 'POST', body: formData })
        .then(res => res.json())
        .then(data => {
          if (data.success) {
            showToast('User created successfully', 'success');
            setTimeout(() => location.reload(), 800);
          } else {
            showToast(data.message || 'Error creating user', 'error');
          }
        });
    });
  }

  const confirmBtn = document.getElementById('confirmButton');
  if (confirmBtn) {
    confirmBtn.addEventListener('click', confirmAction);
  }
}

// --- TABLE FILTERING WITH DEBOUNCE ---
function filterUsersTable() {
  const searchTerm = (document.getElementById('searchInput')?.value || '').toLowerCase();
  const roleFilter = document.getElementById('roleFilter')?.value || '';
  const statusFilter = document.getElementById('statusFilter')?.value || '';

  const rows = document.querySelectorAll('.user-row');
  const defaultEmptyState = document.getElementById('defaultEmptyState');
  const searchEmptyState = document.getElementById('searchEmptyState');

  let visibleCount = 0;

  rows.forEach(row => {
    const username = (row.dataset.username || '').toLowerCase();
    const role = row.dataset.role || '';
    const status = row.dataset.status || '';

    const matchesSearch = !searchTerm || username.includes(searchTerm);
    const matchesRole = !roleFilter || role === roleFilter;
    const matchesStatus = !statusFilter || status === statusFilter;

    if (matchesSearch && matchesRole && matchesStatus) {
      row.style.display = '';
      visibleCount++;
    } else {
      row.style.display = 'none';
    }
  });

  if (visibleCount === 0 && rows.length > 0) {
    if (searchEmptyState) searchEmptyState.classList.remove('hidden');
    if (defaultEmptyState) defaultEmptyState.classList.add('hidden');
  } else {
    if (searchEmptyState) searchEmptyState.classList.add('hidden');
  }
}

// --- UTILITY HELPERS ---
function debounce(func, wait) {
  let timeout;
  return function executedFunction(...args) {
    const later = () => {
      clearTimeout(timeout);
      func(...args);
    };
    clearTimeout(timeout);
    timeout = setTimeout(later, wait);
  };
}

function escapeHtml(str) {
  if (!str) return '';
  return str.replace(/[&<>"']/g, (m) => ({
    '&': '&amp;',
    '<': '&lt;',
    '>': '&gt;',
    '"': '&quot;',
    "'": '&#039;'
  })[m]);
}

function formatFileSize(bytes) {
  if (!bytes || bytes === 0) return '0 B';
  const k = 1024;
  const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
}

function formatDuration(seconds) {
  if (!seconds || seconds === 0) return '00:00:00';
  const totalSeconds = Math.floor(parseFloat(seconds));
  const hours = Math.floor(totalSeconds / 3600);
  const minutes = Math.floor((totalSeconds % 3600) / 60);
  const secs = totalSeconds % 60;
  return `${hours.toString().padStart(2, '0')}:${minutes.toString().padStart(2, '0')}:${secs.toString().padStart(2, '0')}`;
}

function showToast(message, type = 'success') {
  if (typeof window.showToast === 'function') {
    window.showToast(message, type);
    return;
  }
  const toast = document.createElement('div');
  toast.className = `fixed bottom-5 right-5 z-[9999] px-4 py-3 rounded-xl shadow-2xl text-white text-xs font-semibold flex items-center space-x-2 transition-all transform translate-y-2 ${type === 'success' ? 'bg-emerald-600' : 'bg-rose-600'}`;
  toast.innerHTML = `<i class="ti ${type === 'success' ? 'ti-check' : 'ti-alert-circle'} text-base"></i><span>${escapeHtml(message)}</span>`;
  document.body.appendChild(toast);
  setTimeout(() => {
    toast.style.opacity = '0';
    setTimeout(() => toast.remove(), 300);
  }, 3000);
}
