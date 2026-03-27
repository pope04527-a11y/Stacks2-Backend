// Admin settings loader/saver for /admin-panel/settings.html
// - Publicly fetched script (no auth on script fetch).
// - Uses localStorage.adminToken and sends it as 'x-admin-token' for admin API calls.

(async function () {
  const token = localStorage.getItem('adminToken');
  if (!token) {
    // allow the script to load but redirect actions to login when token is missing
    console.warn('admin token not found in localStorage');
  }

  const $ = id => document.getElementById(id);
  const showToast = (msg, ok = true) => {
    const t = $('toast');
    if (!t) return;
    t.textContent = msg;
    t.style.backgroundColor = ok ? '#1767c6' : '#e86128';
    t.className = 'toast show';
    setTimeout(() => { t.className = 'toast'; }, 3000);
  };

  async function apiGet() {
    const res = await fetch('/admin/settings', {
      headers: { 'x-admin-token': token || '' }
    });
    if (res.status === 401 || res.status === 403) {
      localStorage.removeItem('adminToken');
      alert('Session expired. Please login again.');
      window.location.href = '/admin-panel/login.html';
      throw new Error('unauthorized');
    }
    if (!res.ok) throw new Error('Failed to load settings');
    return res.json();
  }

  async function apiSave(payload) {
    const res = await fetch('/admin/settings', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-admin-token': token || ''
      },
      body: JSON.stringify(payload)
    });
    if (res.status === 401 || res.status === 403) {
      localStorage.removeItem('adminToken');
      alert('Session expired. Please login again.');
      window.location.href = '/admin-panel/login.html';
      throw new Error('unauthorized');
    }
    if (!res.ok) {
      const body = await res.json().catch(() => ({}));
      throw new Error(body.message || 'Failed to save settings');
    }
    return res.json();
  }

  function toArrayCSV(s) {
    if (!s) return [];
    if (Array.isArray(s)) return s;
    return String(s).split(',').map(x => x.trim()).filter(Boolean);
  }
  function fromArray(a) {
    if (!Array.isArray(a)) return '';
    return a.join(',');
  }

  function populate(settings) {
    try {
      if (!settings) return;
      $('siteName').value = settings.siteName || '';
      $('currency').value = settings.currency || '';
      $('defaultVip').value = settings.defaultVip ?? 1;
      $('inviteBonus').value = settings.inviteBonus ?? '';
      $('telegramGroup').value = settings.telegramGroup || '';
      $('homepageNotice').value = settings.homepageNotice || '';
      $('depositInstructions').value = settings.depositInstructions || '';
      $('withdrawInstructions').value = settings.withdrawInstructions || '';
      $('minWithdraw').value = settings.minWithdraw ?? '';
      $('maxWithdraw').value = settings.maxWithdraw ?? '';
      $('withdrawFeePercent').value = (settings.withdrawFeePercent ?? settings.withdrawFee) ?? '';
      $('minDeposit').value = settings.minDeposit ?? '';
      $('maxDeposit').value = settings.maxDeposit ?? '';
      $('dailyTaskSet').value = settings.dailyTaskSet ?? '';
      $('maintenance').checked = !!settings.maintenance;
      $('maintenanceMode').checked = !!settings.maintenanceMode;
      $('whatsappLink').value = (settings.service && settings.service.whatsapp) || '';
      $('telegramLink').value = (settings.service && settings.service.telegram) || '';
      $('activityLockEnabled').checked = !!(settings.activityLock && settings.activityLock.enabled);
      $('lockedUsers').value = (settings.activityLock && Array.isArray(settings.activityLock.users)) ? settings.activityLock.users.join(',') : '';
      // Platform closing
      $('platformClosed').checked = !!settings.platformClosed;
      $('autoOpenTime').value = settings.autoOpenTime || (typeof settings.autoOpenHourUK === 'number' ? String(settings.autoOpenHourUK).padStart(2,'0') + ':00' : '');
      $('allowList').value = Array.isArray(settings.allowList) ? settings.allowList.join(',') : (Array.isArray(settings.whoCanAccessDuringClose) ? settings.whoCanAccessDuringClose.join(',') : '');
    } catch (err) {
      console.error('populate error', err);
    }
  }

  function gather() {
    const service = { whatsapp: $('whatsappLink').value.trim(), telegram: $('telegramLink').value.trim() };
    const activityUsers = toArrayCSV($('lockedUsers').value);
    const allowList = toArrayCSV($('allowList').value);

    const payload = {
      siteName: $('siteName').value.trim(),
      currency: $('currency').value.trim(),
      defaultVip: Number($('defaultVip').value) || 1,
      inviteBonus: Number($('inviteBonus').value) || 0,
      telegramGroup: $('telegramGroup').value.trim(),
      homepageNotice: $('homepageNotice').value,
      depositInstructions: $('depositInstructions').value,
      withdrawInstructions: $('withdrawInstructions').value,
      minWithdraw: Number($('minWithdraw').value) || 0,
      maxWithdraw: Number($('maxWithdraw').value) || 0,
      withdrawFeePercent: Number($('withdrawFeePercent').value) || 0,
      minDeposit: Number($('minDeposit').value) || 0,
      maxDeposit: Number($('maxDeposit').value) || 0,
      dailyTaskSet: Number($('dailyTaskSet').value) || 0,
      maintenance: !!$('maintenance').checked,
      maintenanceMode: !!$('maintenanceMode').checked,
      service,
      activityLock: {
        enabled: !!$('activityLockEnabled').checked,
        users: activityUsers
      },
      platformClosed: !!$('platformClosed').checked,
      autoOpenTime: $('autoOpenTime').value || undefined,
      allowList
    };

    // strip undefined
    Object.keys(payload).forEach(k => payload[k] === undefined && delete payload[k]);
    return payload;
  }

  // load
  try {
    const s = await apiGet();
    populate(s);
  } catch (err) {
    console.error('Failed to load settings:', err);
    showToast('Failed to load settings', false);
  }

  // submit handler
  const form = $('settingsForm');
  if (form) {
    form.addEventListener('submit', async (e) => {
      e.preventDefault();
      const payload = gather();
      try {
        await apiSave(payload);
        showToast('Settings saved');
        $('lastAction').innerHTML = 'Saved at ' + new Date().toLocaleString();
        // refresh current values
        const s = await apiGet();
        populate(s);
      } catch (err) {
        console.error('save error', err);
        showToast(err.message || 'Save failed', false);
      }
    });
  }
})();
