  import { initializeApp } from "https://www.gstatic.com/firebasejs/10.12.0/firebase-app.js";
  import {
    getFirestore, collection, doc, setDoc, getDoc, addDoc,
    onSnapshot, query, orderBy, serverTimestamp, deleteDoc,
    updateDoc, increment, getDocs, writeBatch
  } from "https://www.gstatic.com/firebasejs/10.12.0/firebase-firestore.js";
  import {
    getDatabase, ref, onValue, onDisconnect, set, remove, serverTimestamp as rtServerTimestamp
  } from "https://www.gstatic.com/firebasejs/10.12.0/firebase-database.js";

  // ── Fetch Firebase config from your Render backend ──────────
  const CONFIG_URL = "https://vault-chat-backend.onrender.com/api/firebase-config";

  let db, rtdb;

  async function initFirebase() {
    try {
      const res = await fetch(CONFIG_URL);
      if (!res.ok) throw new Error("Failed to fetch config: " + res.status);
      const firebaseConfig = await res.json();
      const app = initializeApp(firebaseConfig);
      db   = getFirestore(app);
      rtdb = getDatabase(app);
    } catch (e) {
      console.error("Firebase init failed:", e);
      alert("Could not connect to VaultChat services. Please try again later.");
    }
  }

  window.addEventListener("beforeunload", () => {
    if (presenceRef) {
      navigator.sendBeacon && navigator.sendBeacon("", "");
      // onDisconnect already handles RTDB removal
    }
  });

  await initFirebase();

  // ── Simple hash (SHA-256 via SubtleCrypto) ──────────────────
  async function hashPassword(password) {
    const encoder = new TextEncoder();
    const data = encoder.encode(password);
    const hashBuffer = await crypto.subtle.digest("SHA-256", data);
    const hashArray  = Array.from(new Uint8Array(hashBuffer));
    return hashArray.map(b => b.toString(16).padStart(2, "0")).join("");
  }

  // ── Sanitise text input ─────────────────────────────────────
  function sanitize(str) {
    const div = document.createElement("div");
    div.textContent = str.trim();
    return div.innerHTML;
  }

  // ── Generate random room ID ─────────────────────────────────
  function generateRoomId() {
    const chars = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789";
    let id = "";
    for (let i = 0; i < 10; i++) id += chars[Math.floor(Math.random() * chars.length)];
    return id;
  }

// ── Rate Limiting State ──────────────────────────────────────
  const rateLimitState = {
    msgTimestamps: [],
    cooldownUntil: 0,
    violationLevel: 0,
    lastSendTime: 0,
    pendingMsg: null,
    dupHistory: [],
    dupViolations: 0,
    roomAttempts: {},
    roomLockouts: {},
    connCount: 0,
  };


  function checkRateLimit() {
    const now = Date.now();
    rateLimitState.msgTimestamps = rateLimitState.msgTimestamps.filter(t => now - t < 60000);
    const last10s = rateLimitState.msgTimestamps.filter(t => now - t < 10000).length;
    const last1s  = rateLimitState.msgTimestamps.filter(t => now - t < 1000).length;
    const last60s = rateLimitState.msgTimestamps.length;
    if (last1s >= 3 || last10s >= 15 || last60s >= 50) {
      rateLimitState.violationLevel = Math.min(rateLimitState.violationLevel + 1, 3);
      const delays = [1000, 3000, 10000];
      return delays[rateLimitState.violationLevel - 1] || 10000;
    }
    const timeSinceLast = now - rateLimitState.lastSendTime;
    if (timeSinceLast < 300) return 300 - timeSinceLast;
    if (rateLimitState.violationLevel > 0 && last60s === 0 && last10s === 0) {
      rateLimitState.violationLevel = 0;
    }
    return 0;
  }

  function checkDuplicate(text) {
    const now = Date.now();
    rateLimitState.dupHistory = rateLimitState.dupHistory.filter(e => now - e.time < 60000);
    const dupes = rateLimitState.dupHistory.filter(e => e.text === text).length;
    rateLimitState.dupHistory.push({ text, time: now });
    if (dupes >= 10) return 5000;
    if (dupes >= 5)  return 2000;
    return 0;
  }

  function checkRoomCreationLimit() {
    const now = Date.now();
    const ip  = "local";
    if (!rateLimitState.roomAttempts[ip]) rateLimitState.roomAttempts[ip] = [];
    rateLimitState.roomAttempts[ip] = rateLimitState.roomAttempts[ip].filter(t => now - t < 60000);
    if (rateLimitState.roomAttempts[ip].length >= 3) return false;
    rateLimitState.roomAttempts[ip].push(now);
    return true;
  }

  function getRoomLockout(roomId) {
    const entry = rateLimitState.roomLockouts[roomId];
    if (!entry) return null;
    if (Date.now() < entry.until) return entry;
    return null;
  }

  function recordFailedAttempt(roomId) {
    if (!rateLimitState.roomLockouts[roomId]) {
      rateLimitState.roomLockouts[roomId] = { failures: 0, until: 0, level: 0 };
    }
    const entry = rateLimitState.roomLockouts[roomId];
    entry.failures++;
    if (entry.failures >= 3) {
      const durations = [5 * 60000, 15 * 60000, 30 * 60000, 60 * 60000];
      entry.until = Date.now() + (durations[entry.level] || 60 * 60000);
      entry.level = Math.min(entry.level + 1, durations.length - 1);
      entry.failures = 0;
    }
  }

  function clearLockout(roomId) {
    delete rateLimitState.roomLockouts[roomId];
  }
  
  // ── Global session state ────────────────────────────────────
  let currentRoomId   = null;
  let currentUsername = null;
  let presenceRef     = null;
  let unsubMessages   = null;
  let unsubPresence   = null;
  let inactivityTimer = null;
  let shieldTimer     = null;
  let replyTo             = null;
  let allRoomUsers        = {};
  let roomCreatedAt       = null;
  let firstLoad           = true;
  let myPresenceUid       = null;
  let activityTimer       = null;
  const INACTIVITY_MS     = 3 * 60 * 1000;
  const OFFLINE_DELAY_MS  = 10 * 1000;
  const ONLINE_INSTANT    = true;

  // ── Page navigation ─────────────────────────────────────────
  function showPage(id) {
    document.querySelectorAll(".page").forEach(p => p.classList.remove("active"));
    const el = document.getElementById(id);
    if (el) { el.classList.add("active"); }
  }

  // ── LANDING → SHIELD ANIMATION → ACCESS SELECTION ──────────
  window.startApp = function () {
    showPage("page-shield");
    const shield = document.getElementById("shield-svg");
    shield.classList.remove("animate");
    void shield.offsetWidth;
    shield.classList.add("animate");
    shieldTimer = setTimeout(() => showPage("page-access"), 2800);
  };

  // ── ACCESS SELECTION ────────────────────────────────────────
  window.goAdmin = function () { showPage("page-admin"); };
  window.goJoin  = function () { showPage("page-join");  };
  window.goBack  = function (target) {
    if (shieldTimer) { clearTimeout(shieldTimer); shieldTimer = null; }
    showPage(target || "page-access");
  };

  // ── GENERATE ROOM ID ────────────────────────────────────────
  window.generateId = function () {
    document.getElementById("admin-room-id").value = generateRoomId();
  };

  // ── CREATE ROOM (ADMIN) ─────────────────────────────────────
  window.createRoom = async function () {
    const username  = sanitize(document.getElementById("admin-username").value);
    const roomId    = sanitize(document.getElementById("admin-room-id").value).toUpperCase();
    const password  = document.getElementById("admin-password").value;
    const confirm   = document.getElementById("admin-confirm").value;
    const errEl     = document.getElementById("admin-error");

    errEl.textContent = "";

    if (!username)                          return setErr(errEl, "Username is required.");
    if (username.length > 24)               return setErr(errEl, "Username must be 24 characters or fewer.");
    if (!roomId || roomId.length < 4)       return setErr(errEl, "Room ID must be at least 4 characters.");
    if (!password || password.length < 6)   return setErr(errEl, "Password must be at least 6 characters.");
    if (password !== confirm)               return setErr(errEl, "Passwords do not match.");

    if (!checkRoomCreationLimit()) {
      return setErr(errEl, "Too many rooms created. Please wait 60 seconds.");
    }

    setLoading("btn-create", true);
    try {
      const roomRef  = doc(db, "rooms", roomId);
      const roomSnap = await getDoc(roomRef);
      if (roomSnap.exists()) return setErr(errEl, "Room ID already in use. Try another.");

      const hashed = await hashPassword(password);
      await setDoc(roomRef, {
        createdAt:    serverTimestamp(),
        passwordHash: hashed,
        admin:        username,
        active:       true
      });

      currentRoomId   = roomId;
      currentUsername = username;
      enterChatRoom(new Date());
    } catch (e) {
      setErr(errEl, "Failed to create room: " + e.message);
    } finally {
      setLoading("btn-create", false);
    }
  };

  // ── JOIN ROOM ────────────────────────────────────────────────
  window.joinRoom = async function () {
    const username = sanitize(document.getElementById("join-username").value);
    const roomId   = sanitize(document.getElementById("join-room-id").value).toUpperCase();
    const password = document.getElementById("join-password").value;
    const errEl    = document.getElementById("join-error");

    errEl.textContent = "";

    if (!username)                  return setErr(errEl, "Username is required.");
    if (username.length > 24)       return setErr(errEl, "Username must be 24 characters or fewer.");
    if (!roomId)                    return setErr(errEl, "Room ID is required.");
    if (!password)                  return setErr(errEl, "Password is required.");

    const lockout = getRoomLockout(roomId);
    if (lockout) {
      const remaining = Math.ceil((lockout.until - Date.now()) / 60000);
      return setErr(errEl, `Too many failed attempts. Try again in ${remaining} minute(s).`);
    }

    setLoading("btn-join", true);
    try {
      const roomSnap = await getDoc(doc(db, "rooms", roomId));
      if (!roomSnap.exists()) return setErr(errEl, "Room not found. Check the Room ID.");

      const hashed = await hashPassword(password);
      if (roomSnap.data().passwordHash !== hashed) {
        recordFailedAttempt(roomId);
        const lo = getRoomLockout(roomId);
        if (lo) {
          const remaining = Math.ceil((lo.until - Date.now()) / 60000);
          return setErr(errEl, `Too many failed attempts. Locked for ${remaining} minute(s).`);
        }
        return setErr(errEl, "Incorrect password.");
      }

      clearLockout(roomId);
      currentRoomId   = roomId;
      currentUsername = username;
      const createdAt = roomSnap.data().createdAt ? roomSnap.data().createdAt.toDate() : new Date();
      enterChatRoom(createdAt);
    } catch (e) {
      setErr(errEl, "Failed to join room: " + e.message);
    } finally {
      setLoading("btn-join", false);
    }
  };

  // ── ENTER CHAT ROOM ──────────────────────────────────────────
  function enterChatRoom(createdAt) {
    roomCreatedAt = createdAt || null;
    document.getElementById("chat-room-id-text").textContent = currentRoomId;
    document.getElementById("chat-username").textContent     = currentUsername;
    showPage("page-chat");
    setupPresence();
    listenMessages();
    setupScrollBtn();
    setupTypingListener();
    setupPinnedListener();
  }

  // ── PRESENCE (Realtime DB) ───────────────────────────────────
  function setupPresence() {
    myPresenceUid = Math.random().toString(36).slice(2);
    presenceRef = ref(rtdb, `presence/${currentRoomId}/${myPresenceUid}`);

    set(presenceRef, { username: currentUsername, joinedAt: rtServerTimestamp(), online: true, lastSeen: rtServerTimestamp() });
    onDisconnect(presenceRef).remove();
    postJoinMessage(currentUsername);
    startActivityTracking();

    const roomPresenceRef = ref(rtdb, `presence/${currentRoomId}`);
    unsubPresence = onValue(roomPresenceRef, snapshot => {
      const data = snapshot.exists() ? snapshot.val() : {};

      // Merge every presence node into allRoomUsers using the online field
      Object.values(data).forEach(u => {
        if (!u.username) return;
        allRoomUsers[u.username] = { online: u.online === true };
      });

      // Anyone no longer in the DB at all → offline
      const usernamesInDB = new Set(Object.values(data).map(u => u.username).filter(Boolean));
      Object.keys(allRoomUsers).forEach(name => {
        if (!usernamesInDB.has(name)) allRoomUsers[name] = { online: false };
      });

      // Count only nodes where online === true
      const onlineCount = Object.values(data).filter(u => u.online === true).length;
      document.getElementById("user-count").textContent = onlineCount;

      const panel = document.getElementById("room-info-panel");
      if (panel && panel.classList.contains("open")) renderParticipantList();

      // Inactivity: fire only when zero nodes exist in DB at all
      const totalInDB = Object.keys(data).length;
      if (totalInDB === 0) {
        clearTimeout(inactivityTimer);
        inactivityTimer = setTimeout(() => deleteRoom(currentRoomId), INACTIVITY_MS);
      } else {
        clearTimeout(inactivityTimer);
      }
    });
  }

  // ── POST JOIN SYSTEM MESSAGE ─────────────────────────────────
  async function postJoinMessage(username) {
    const now  = new Date();
    const time = now.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
    try {
      await addDoc(collection(db, "rooms", currentRoomId, "messages"), {
        text:      `${username} joined the room at ${time}`,
        system:    true,
        timestamp: serverTimestamp()
      });
    } catch (e) { /* silent */ }
  }

  // ── LISTEN TO MESSAGES (Firestore) ──────────────────────────
  function listenMessages() {
    firstLoad = true;
    const msgQuery = query(
      collection(db, "rooms", currentRoomId, "messages"),
      orderBy("timestamp", "asc")
    );
    unsubMessages = onSnapshot(msgQuery, snapshot => {
      if (firstLoad) {
        renderMessage({ system: true, text: "Secure room established — messages are ephemeral" });
        firstLoad = false;
      }
      snapshot.docChanges().forEach(change => {
        if (change.type === "added") {
          const d = change.doc.data();
          d.msgId = change.doc.id;
          renderMessage(d);
        }
      });
      scrollChat();
    });
  }

  // ── SEND MESSAGE ─────────────────────────────────────────────
  window.sendMessage = async function () {
    const input = document.getElementById("msg-input");
    const text  = input.value.trim();
    if (!text) return;
    if (text.length > 2000) { showToast("Message too long (max 2000 characters)."); return; }

    const delay1 = checkRateLimit();
    const delay2 = checkDuplicate(text);
    const delay  = Math.max(delay1, delay2);

    if (delay > 0) {
      showToast(`Slow down — sending in ${(delay / 1000).toFixed(1)}s…`);
      input.disabled = true;
      await new Promise(r => setTimeout(r, delay));
      input.disabled = false;
      input.focus();
    }

    input.value = "";
    const safe = sanitize(text);
    if (!safe) return;

    rateLimitState.msgTimestamps.push(Date.now());
    rateLimitState.lastSendTime = Date.now();

    const msgData = {
      text:      safe,
      username:  currentUsername,
      displayName: currentUsername,
      timestamp: serverTimestamp()
    };
    if (replyTo) {
      msgData.replyTo = { username: replyTo.username, text: replyTo.text };
    }
    clearReply();
    clearTypingStatus();

    try {
      await addDoc(collection(db, "rooms", currentRoomId, "messages"), msgData);
    } catch (e) {
      console.error("Send failed:", e);
    }
  };

  // ── REPLY HELPERS ────────────────────────────────────────────
  window.setReply = function (username, text) {
    username = username.replace(/ \(IP - [\d.a-fA-F.:]+\)$/, "");
    replyTo = { username, text };
    const bar       = document.getElementById("reply-bar");
    const nameEl    = document.getElementById("reply-name");
    const previewEl = document.getElementById("reply-preview");
    nameEl.textContent    = username === currentUsername ? "You" : username;
    previewEl.textContent = text.length > 60 ? text.slice(0, 60) + "..." : text;
    bar.classList.add("visible");
    document.getElementById("msg-input").focus();
  };

  window.clearReply = function () {
    replyTo = null;
    document.getElementById("reply-bar").classList.remove("visible");
  };

  // Allow Enter key to send
  window.handleMsgKey = function (e) {
    if (e.key === "Enter" && !e.shiftKey) { e.preventDefault(); window.sendMessage(); }
  };

  // ── RENDER MESSAGE ───────────────────────────────────────────
  function formatTime(ts) {
    if (!ts) return "";
    const d = ts.toDate ? ts.toDate() : new Date(ts);
    return d.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
  }

  function renderMessage(data) {
    const feed  = document.getElementById("msg-feed");

    const empty = document.getElementById("empty-state");
    if (empty) empty.style.display = "none";

    // ── System message ──────────────────────────────────────
    if (data.system) {
      const sys = document.createElement("div");
      sys.className = "msg-system";
      sys.textContent = data.text;
      feed.appendChild(sys);
      scrollChat();
      return;
    }

    const storedName  = data.displayName || data.username;
    const rawUsername = data.username || "";
    const baseUsername = rawUsername.replace(/ \(IP - [\d.]+\)$/, "");
    const isMine      = baseUsername === currentUsername || storedName === currentUsername;
    const displayName = isMine ? "You" : (data.displayName || baseUsername);
    const timeStr     = formatTime(data.timestamp);

    const _ukey = data.displayName || data.username.replace(/ \(IP - [\d.]+\)$/, "");
    if (!allRoomUsers[_ukey]) allRoomUsers[_ukey] = { online: false };

    // ── Wrapper ─────────────────────────────────────────────
    const wrap = document.createElement("div");
    wrap.className = `msg-wrap ${isMine ? "mine" : "theirs"}`;
    if (data.msgId) wrap.id = `msg-${data.msgId}`;

    // ── Name + time row ─────────────────────────────────────
    const meta = document.createElement("div");
    meta.className = "msg-meta";

    const label = document.createElement("span");
    label.className = "msg-label";
    label.textContent = displayName;

    const timeEl = document.createElement("span");
    timeEl.className = "msg-time";
    timeEl.textContent = timeStr;

    meta.appendChild(label);
    meta.appendChild(timeEl);

    // ── Reply quote ─────────────────────────────────────────
    if (data.replyTo) {
      const quote = document.createElement("div");
      quote.className = "msg-quote";
      const quoteName = document.createElement("span");
      quoteName.className = "msg-quote-name";
      const replyDisplayName = data.replyTo.username.replace(/ \(IP - [\d.a-fA-F.:]+\)$/, "");
      quoteName.textContent = replyDisplayName === currentUsername ? "You" : replyDisplayName;
      const quoteText = document.createElement("span");
      quoteText.className = "msg-quote-text";
      quoteText.textContent = data.replyTo.text.length > 60
        ? data.replyTo.text.slice(0, 60) + "..." : data.replyTo.text;
      quote.appendChild(quoteName);
      quote.appendChild(quoteText);
      wrap.appendChild(meta);
      wrap.appendChild(quote);
    } else {
      wrap.appendChild(meta);
    }

    // ── Bubble ───────────────────────────────────────────────
    const bubble = document.createElement("div");
    bubble.className = "msg-bubble";
    bubble.textContent = data.text;

    bubble.addEventListener("contextmenu", async (e) => {
      e.preventDefault();
      try { await navigator.clipboard.writeText(data.text); showToast("Message copied."); }
      catch { showToast("Could not copy."); }
    });

    let pressTimer;
    bubble.addEventListener("touchstart", () => {
      pressTimer = setTimeout(async () => {
        try { await navigator.clipboard.writeText(data.text); showToast("Message copied."); }
        catch { showToast("Could not copy."); }
      }, 600);
    });
    bubble.addEventListener("touchend",  () => clearTimeout(pressTimer));
    bubble.addEventListener("touchmove", () => clearTimeout(pressTimer));

    // ── Reply button ─────────────────────────────────────────
    const replyBtn = document.createElement("button");
    replyBtn.className = "msg-reply-btn";
    replyBtn.title = "Reply";
    replyBtn.innerHTML = `<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"><polyline points="9 17 4 12 9 7"/><path d="M20 18v-2a4 4 0 0 0-4-4H4"/></svg>`;
    replyBtn.onclick = () => window.setReply(data.username, data.text);
    
    const pinBtn = document.createElement("button");
    pinBtn.className = "msg-pin-btn";
    pinBtn.title = "Pin message";
    pinBtn.innerHTML = `<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"><path d="M12 2v7"/><path d="M5 9h14"/><path d="M9 9v5a3 3 0 0 0 6 0V9"/><line x1="12" y1="16" x2="12" y2="22"/></svg>`;
    pinBtn.onclick = () => window.pinMessage(data.msgId || "", data.text, data.username);

    const bubbleRow = document.createElement("div");
    bubbleRow.className = "msg-bubble-row";
    if (isMine) {
      bubbleRow.appendChild(pinBtn);
      bubbleRow.appendChild(replyBtn);
      bubbleRow.appendChild(bubble);
    } else {
      bubbleRow.appendChild(bubble);
      bubbleRow.appendChild(replyBtn);
      bubbleRow.appendChild(pinBtn);
    }

    wrap.appendChild(bubbleRow);
    feed.appendChild(wrap);
    scrollChat();
  }

  function scrollChat() {
    const feed = document.getElementById("msg-feed");
    feed.scrollTop = feed.scrollHeight;
    const btn = document.getElementById("scroll-btn");
    if (btn) btn.classList.remove("visible");
  }

  function setupScrollBtn() {
    const feed = document.getElementById("msg-feed");
    const btn  = document.getElementById("scroll-btn");
    if (!feed || !btn) return;
    feed.addEventListener("scroll", () => {
      const atBottom = feed.scrollHeight - feed.scrollTop - feed.clientHeight < 80;
      btn.classList.toggle("visible", !atBottom);
    });
  }

function startActivityTracking() {
  const events = ["mousemove", "keydown", "mousedown", "touchstart", "scroll", "click"];
  events.forEach(evt => window.addEventListener(evt, onUserActivity, { passive: true }));

  // Page visibility: going hidden starts the offline countdown, coming back = instant online
  document.addEventListener("visibilitychange", () => {
    if (document.hidden) {
      scheduleOffline();
    } else {
      onUserActivity();
    }
  });

  // Initial kick — mark online immediately
  setMyOnlineStatus(true);
  resetOfflineCountdown();
}

function onUserActivity() {
  // Instant online — no delay
  if (allRoomUsers[currentUsername] && !allRoomUsers[currentUsername].online) {
    setMyOnlineStatus(true);
  }
  resetOfflineCountdown();
}

function resetOfflineCountdown() {
  clearTimeout(activityTimer);
  activityTimer = setTimeout(scheduleOffline, OFFLINE_DELAY_MS);
}

function scheduleOffline() {
  clearTimeout(activityTimer);
  setMyOnlineStatus(false);
}

function setMyOnlineStatus(isOnline) {
  if (!presenceRef) return;
  const payload = {
    username: currentUsername,
    online:   isOnline,
    lastSeen: rtServerTimestamp()
  };
  // Preserve joinedAt so the node always exists in DB
  if (isOnline) payload.joinedAt = rtServerTimestamp();
  set(presenceRef, payload);
}

  window.scrollToBottom = function () {
    const feed = document.getElementById("msg-feed");
    feed.scrollTo({ top: feed.scrollHeight, behavior: "smooth" });
  };

  // ── SHARE / COPY ROOM ID ─────────────────────────────────────
  window.shareRoom = async function () {
    const shareData = {
      title: "VaultChat — Join My Room",
      text:  `Join my secure VaultChat room!\nRoom ID: ${currentRoomId}`,
      url:   window.location.href
    };
    try {
      if (navigator.share) {
        await navigator.share(shareData);
      } else {
        await navigator.clipboard.writeText(
          `VaultChat Room ID: ${currentRoomId}\n${window.location.href}`
        );
        showToast("Room invite copied to clipboard.");
      }
    } catch { showToast("Room ID: " + currentRoomId); }
  };

  window.copyRoomId = async function () {
    try {
      await navigator.clipboard.writeText(currentRoomId);
      showToast("Room ID copied.");
    } catch { showToast("Room ID: " + currentRoomId); }
  };

  // ── DELETE ROOM ──────────────────────────────────────────────
  async function deleteRoom(roomId) {
    try {
      const msgSnap = await getDocs(collection(db, "rooms", roomId, "messages"));
      const batch   = writeBatch(db);
      msgSnap.forEach(d => batch.delete(d.ref));
      batch.delete(doc(db, "rooms", roomId));
      await batch.commit();
      await remove(ref(rtdb, `presence/${roomId}`));
    } catch (e) { console.error("Room deletion error:", e); }
  }

  // ── ROOM INFO PANEL ──────────────────────────────────────────
  function renderParticipantList() {
    const list = document.getElementById("participant-list");
    if (!list) return;
    list.innerHTML = "";
    const entries = Object.entries(allRoomUsers);
    if (entries.length === 0) {
      list.innerHTML = `<div style="color:var(--text-3);font-size:11px;text-align:center;padding:8px 0;">No participants yet</div>`;
      return;
    }
    entries.sort((a, b) => (b[1].online ? 1 : 0) - (a[1].online ? 1 : 0));
    entries.forEach(([name, info]) => {
      const row = document.createElement("div");
      row.className = "participant-row";

      const dot = document.createElement("span");
      dot.className = `participant-dot ${info.online ? "online" : "offline"}`;

      const nameEl = document.createElement("span");
      nameEl.className = "participant-name";
      nameEl.textContent = name === currentUsername ? `${name} (You)` : name;

      const statusEl = document.createElement("span");
      statusEl.className = "participant-status";
      statusEl.textContent = info.online ? "Online" : "Offline";
      statusEl.style.color = info.online ? "var(--accent2)" : "var(--text-3)";

      row.appendChild(dot);
      row.appendChild(nameEl);
      row.appendChild(statusEl);
      list.appendChild(row);
    });
  }

  window.toggleRoomInfo = function () {
    const panel = document.getElementById("room-info-panel");
    panel.classList.toggle("open");
    if (panel.classList.contains("open")) {
      document.getElementById("info-room-id").textContent    = currentRoomId;
      document.getElementById("info-user-count").textContent = document.getElementById("user-count").textContent;
      document.getElementById("info-created").textContent    =
        roomCreatedAt ? roomCreatedAt.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" }) : "—";
      renderParticipantList();
    }
  };

  // ── LEAVE ROOM ───────────────────────────────────────────────
  window.leaveRoom = async function () {
    await cleanupPresence();
    showPage("page-landing");
  };

  async function cleanupPresence() {
    if (unsubMessages) unsubMessages();
    if (unsubPresence) unsubPresence();
    clearTimeout(inactivityTimer);
    clearTimeout(activityTimer);
    teardownTyping();
    if (pinnedMsgUnsubscribe) { pinnedMsgUnsubscribe(); pinnedMsgUnsubscribe = null; }
    if (presenceRef) {
      onDisconnect(presenceRef).cancel();
      await remove(presenceRef);
      presenceRef = null;
    }
  }

// ── PINNED MESSAGE ───────────────────────────────────────────
  function setupPinnedListener() {
    if (!rtdb || !currentRoomId) return;
    const pinnedRef = ref(rtdb, `pinned/${currentRoomId}`);
    pinnedMsgUnsubscribe = onValue(pinnedRef, snapshot => {
      if (snapshot.exists()) {
        renderPinnedBar(snapshot.val());
      } else {
        clearPinnedBar();
      }
    });
  }

  function renderPinnedBar(data) {
    const bar      = document.getElementById("pinned-bar");
    const textEl   = document.getElementById("pinned-text");
    const authorEl = document.getElementById("pinned-author");
    if (!bar || !textEl || !authorEl) return;

    pinnedDocId = data.msgId || null;

    textEl.textContent   = data.text
      ? (data.text.length > 80 ? data.text.slice(0, 80) + "…" : data.text)
      : "";
    authorEl.textContent = data.pinnedBy ? "pinned by " + data.pinnedBy : "";

    bar.style.display = "flex";
  }

  function clearPinnedBar() {
    const bar = document.getElementById("pinned-bar");
    if (bar) bar.style.display = "none";
    pinnedDocId = null;
  }

  window.pinMessage = async function (msgId, text, username) {
    if (!currentRoomId || !rtdb) return;
    if (!msgId) { showToast("Cannot pin this message."); return; }
    try {
      await set(ref(rtdb, `pinned/${currentRoomId}`), {
        msgId    : msgId,
        text     : text,
        pinnedBy : currentUsername,
        author   : username,
        pinnedAt : rtServerTimestamp()
      });
      showToast("Message pinned.");
    } catch (e) {
      console.error("Pin failed:", e);
      showToast("Could not pin message.");
    }
  };

  window.unpinMessage = async function () {
    if (!currentRoomId || !rtdb) return;
    try {
      await remove(ref(rtdb, `pinned/${currentRoomId}`));
      showToast("Message unpinned.");
    } catch (e) {
      console.error("Unpin failed:", e);
    }
  };

  window.scrollToPinnedMsg = function () {
    if (!pinnedDocId) return;
    const el = document.getElementById("msg-" + pinnedDocId);
    if (!el) { showToast("Message no longer visible."); return; }
    el.scrollIntoView({ behavior: "smooth", block: "center" });
    el.classList.add("msg-highlight");
    setTimeout(() => el.classList.remove("msg-highlight"), 2000);
  };

// ── TYPING INDICATOR ─────────────────────────────────────────
  let pinnedMsgUnsubscribe = null;
  let pinnedDocId          = null;
  let typingRef        = null;   // ref to this user's typing node in RTDB
  let typingTimeout    = null;   // inactivity timer
  let typingUnsubscribe = null;  // RTDB listener unsubscribe
  let isTyping         = false;  // current local state
  const TYPING_TTL_MS  = 3000;  // clear after 3s inactivity

  function setupTypingListener() {
    if (!rtdb || !currentRoomId) return;
    const roomTypingRef = ref(rtdb, `typing/${currentRoomId}`);
    typingUnsubscribe = onValue(roomTypingRef, snapshot => {
      const data = snapshot.exists() ? snapshot.val() : {};
      const others = Object.values(data)
        .filter(entry => entry && entry.username && entry.username !== currentUsername)
        .map(e => e.username);
      renderTypingIndicator(others);
    }, e => console.error("typing listener error:", e));
  }

  function renderTypingIndicator(names) {
    const bar    = document.getElementById("typing-indicator-bar");
    const nameEl = document.getElementById("typing-names");
    const dots   = document.getElementById("typing-dots");
    if (!bar || !nameEl || !dots) return;

    if (names.length === 0) {
      nameEl.textContent = "";
      dots.classList.remove("active");
      bar.style.visibility = "hidden";
      return;
    }

    let label;
    if (names.length === 1)      label = `${names[0]} is typing`;
    else if (names.length === 2) label = `${names[0]} and ${names[1]} are typing`;
    else                         label = `${names[0]} and ${names.length - 1} others are typing`;

    nameEl.textContent = label;
    dots.classList.add("active");
    bar.style.visibility = "visible";
  }

  function setTypingStatus(status) {
    if (!currentRoomId || !currentUsername || !rtdb) return;
    if (status === isTyping) return;
    isTyping  = status;
    const tRef = ref(rtdb, `typing/${currentRoomId}/${currentUsername}`);
    typingRef  = tRef;
    if (status) {
      set(tRef, { username: currentUsername }).catch(e => console.error("typing set failed:", e));
      onDisconnect(tRef).remove();
    } else {
      remove(tRef).catch(e => console.error("typing remove failed:", e));
    }
  }

  let lastTypingWrite = 0;
  window.handleTypingInput = function () {
    const input = document.getElementById("msg-input");
    if (!input) return;

    clearTimeout(typingTimeout);

    if (input.value.length > 0) {
      const now = Date.now();
      if (now - lastTypingWrite > 1000) {
        lastTypingWrite = now;
        setTypingStatus(true);
      }
      typingTimeout = setTimeout(() => setTypingStatus(false), TYPING_TTL_MS);
    } else {
      setTypingStatus(false);
    }
  };

  function clearTypingStatus() {
    clearTimeout(typingTimeout);
    setTypingStatus(false);
    isTyping = false;
    lastTypingWrite = 0;
  }

  function teardownTyping() {
    clearTypingStatus();
    if (typingUnsubscribe) { typingUnsubscribe(); typingUnsubscribe = null; }
    typingRef = null;
  }

  // ── HELPERS ──────────────────────────────────────────────────
  function setErr(el, msg) {
    el.textContent = msg;
    el.style.opacity = "0";
    requestAnimationFrame(() => { el.style.transition = "opacity 0.3s"; el.style.opacity = "1"; });
  }

  function setLoading(btnId, state) {
    const btn = document.getElementById(btnId);
    if (!btn) return;
    btn.disabled = state;
    btn.textContent = state ? "Processing..." : btn.dataset.label;
  }

  function showToast(msg) {
    const t = document.getElementById("toast");
    t.textContent = msg;
    t.classList.add("show");
    setTimeout(() => t.classList.remove("show"), 3000);
  }

  // ── INIT ─────────────────────────────────────────────────────
  document.addEventListener("DOMContentLoaded", () => {
    document.querySelectorAll("button[id]").forEach(b => b.dataset.label = b.textContent);

    // Close room info panel on any click outside it
    document.addEventListener("click", (e) => {
      const panel   = document.getElementById("room-info-panel");
      const infoBtn = document.querySelector('[onclick="toggleRoomInfo()"]');
      if (!panel || !panel.classList.contains("open")) return;
      if (panel.contains(e.target)) return;
      if (infoBtn && infoBtn.contains(e.target)) return;
      panel.classList.remove("open");
    });

    // Close room info panel on any mouseover outside it
    document.addEventListener("mouseover", (e) => {
      const panel   = document.getElementById("room-info-panel");
      const infoBtn = document.querySelector('[onclick="toggleRoomInfo()"]');
      if (!panel || !panel.classList.contains("open")) return;
      if (panel.contains(e.target)) return;
      if (infoBtn && infoBtn.contains(e.target)) return;
      panel.classList.remove("open");
    });
  });

    // Click-outside to close room info panel
    document.addEventListener("click", (e) => {
      const panel   = document.getElementById("room-info-panel");
      const infoBtn = document.querySelector('[onclick="toggleRoomInfo()"]');
      if (!panel || !panel.classList.contains("open")) return;
      if (panel.contains(e.target)) return;
      if (infoBtn && infoBtn.contains(e.target)) return;
      panel.classList.remove("open");
    });
