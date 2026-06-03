const RT = (typeof browser !== 'undefined') ? browser : chrome;

async function checkPermission() {
  try {
    const has = await RT.permissions.contains({ origins: ['<all_urls>'] });
    const permRow = document.getElementById('permRow');
    const permState = document.getElementById('permState');
    const grantBtn = document.getElementById('grantBtn');
    if (has) {
      permRow.style.display = 'none';
      grantBtn.style.display = 'none';
    } else {
      permRow.style.display = 'flex';
      permState.textContent = 'Host permission missing — click Grant access';
      grantBtn.style.display = 'inline-block';
    }
    return has;
  } catch (e) {
    return false;
  }
}

async function refresh() {
  await checkPermission();
  try {
    const reply = await RT.runtime.sendMessage({ action: 'getStatus' });
    const dot = document.getElementById('dot');
    const state = document.getElementById('state');
    const last = document.getElementById('last');
    if (reply && reply.connected) {
      dot.className = 'dot ok';
      state.textContent = 'Connected to server.py';
    } else {
      dot.className = 'dot off';
      state.textContent = 'Disconnected (auto-retrying)';
    }
    if (reply && reply.lastBatch) {
      last.textContent = `last batch: ${reply.lastBatch}`;
    }
  } catch (e) {
    document.getElementById('state').textContent = 'background not ready';
  }
}

document.getElementById('grantBtn').addEventListener('click', async () => {
  // Firefox MV3 only honors permissions.request from a user gesture.
  try {
    const granted = await RT.permissions.request({ origins: ['<all_urls>'] });
    if (granted) {
      document.getElementById('permState').textContent = 'Granted ✓ — reconnecting once...';
      // Tell background to reconnect WS so server.py sees a fresh connection.
      try { await RT.runtime.sendMessage({ action: 'reconnectWs' }); } catch (_) {}
      setTimeout(refresh, 800);
    } else {
      document.getElementById('permState').textContent = 'Permission denied';
    }
  } catch (e) {
    document.getElementById('permState').textContent = 'Error: ' + (e && e.message);
  }
});

refresh();
setInterval(refresh, 2000);
