const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

// Load the local .env file without exposing secrets to the browser.
try {
  const envText = fs.readFileSync(path.join(__dirname, '.env'), 'utf8');
  for (const rawLine of envText.split(/\r?\n/)) {
    const line = rawLine.trim();
    if (!line || line.startsWith('#')) continue;
    const separator = line.indexOf('=');
    if (separator < 1) continue;
    const key = line.slice(0, separator).trim();
    if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(key) || process.env[key] !== undefined) continue;
    let value = line.slice(separator + 1).trim();
    if ((value.startsWith('"') && value.endsWith('"')) || (value.startsWith("'") && value.endsWith("'"))) value = value.slice(1, -1);
    process.env[key] = value;
  }
} catch (error) {
  if (error.code !== 'ENOENT') console.error('Could not read .env:', error.message);
}

const app = express();
const server = http.createServer(app);
const io = new Server(server, { cors: { origin: false } });
const PORT = Number(process.env.PORT || 3000);
const DATA_FILE = process.env.DATA_FILE || path.join(__dirname, 'data.json');
let persisted = { users: [], rooms: [] };
try { if (fs.existsSync(DATA_FILE)) persisted = JSON.parse(fs.readFileSync(DATA_FILE, 'utf8')); } catch (e) { console.error('Could not read data.json:', e.message); }
const users = new Map((persisted.users || []).map(u => [u.id, u]));
const rooms = new Map((persisted.rooms || []).map(r => [String(r.id), r]));
const communityPosts = Array.isArray(persisted.posts) ? persisted.posts : [];
const studyPingAt = new Map();
const planCooldown = new Map();
const socketUsers = new Map();
const roomMembers = new Map();
function save() { fs.writeFileSync(DATA_FILE, JSON.stringify({ users: [...users.values()], rooms: [...rooms.values()], posts: communityPosts }, null, 2)); }
function safeUser(u) { return { id: u.id, name: u.name }; }
function onlineList() { const ids = new Set(socketUsers.values()); return [...ids].map(id => users.get(id)).filter(Boolean).map(u => ({ ...safeUser(u), online: true })); }
function broadcastOnline() { io.emit('online_users', onlineList()); }
function roomCounts() { const result = {}; for (const [id, members] of roomMembers) result[id] = members.size; return result; }
function broadcastMembers(roomId) {
  const members = [...(roomMembers.get(String(roomId)) || new Map()).values()];
  io.to('room:' + roomId).emit('room_members', { roomId: String(roomId), members });
  io.emit('room_count', { roomId: String(roomId), count: members.length });
}
function catalog() { io.emit('room_catalog', [...rooms.values()]); }
function publicPost(post) { return { ...post, answers: post.answers || [] }; }
function sendCommunity(socket) { socket.emit('community_list', communityPosts.slice(-100).reverse().map(publicPost)); }
function sendLeaderboards(target = io) {
  const all = [...users.values()];
  target.emit('leaderboards', {
    study: all.slice().sort((a,b)=>(b.studySeconds||0)-(a.studySeconds||0)).slice(0,10).map(u=>({id:u.id,name:u.name,score:Math.floor((u.studySeconds||0)/60)})),
    help: all.slice().sort((a,b)=>(b.helpCount||0)-(a.helpCount||0)).slice(0,10).map(u=>({id:u.id,name:u.name,score:u.helpCount||0}))
  });
}
function localPlan(goal, minutes, focusMinutes = 35, restMinutes = 5) {
  const chunks=[]; let left=minutes, n=1;
  while(left>0){const focus=Math.min(focusMinutes,left);chunks.push({title:`第 ${n} 轮：专注完成「${goal}」`,minutes:focus,kind:'focus'});left-=focus;n++;if(left>0){const rest=Math.min(restMinutes,left);chunks.push({title:'离开座位，喝水并休息眼睛',minutes:rest,kind:'break'});left-=rest;}}
  return chunks;
}
async function createStudyPlan(goal, minutes, focusMinutes = 35, restMinutes = 5) {
  const fallback=localPlan(goal,minutes,focusMinutes,restMinutes); const key=process.env.OPENAI_API_KEY;
  if(!key)return {schedule:fallback,source:'local'};
  try {
    const response=await fetch('https://api.openai.com/v1/responses',{method:'POST',headers:{'Authorization':`Bearer ${key}`,'Content-Type':'application/json'},signal:AbortSignal.timeout(25000),body:JSON.stringify({model:process.env.OPENAI_MODEL||'gpt-4o-mini',instructions:'你是细致的学习计划助手。先识别目标中的任务类型、工作量、难点和先后依赖，再拆成具体可检查的小任务，按优先级与合理耗时安排。结合用户偏好的专注与休息节奏，安排短休息；不要臆造用户未提供的信息。只返回 JSON 对象，格式为 {"schedule":[{"title":"...","minutes":25,"kind":"focus"或"break"}]}。总分钟数必须准确，安排短休息，步骤具体但简洁。',input:`学习目标：${goal}
可用总时间：${minutes} 分钟
建议每轮专注：${focusMinutes} 分钟
建议每次休息：${restMinutes} 分钟`,text:{format:{type:'json_object'}}})});
    if(!response.ok)throw new Error('OpenAI request failed'); const data=await response.json();
    const text=(data.output||[]).flatMap(x=>x.content||[]).filter(x=>x.type==='output_text').map(x=>x.text).join('');
    const parsed=JSON.parse(text); const schedule=parsed.schedule;
    if(!Array.isArray(schedule)||!schedule.length||schedule.some(x=>!x.title||!Number.isFinite(x.minutes)||x.minutes<=0)||schedule.reduce((n,x)=>n+Number(x.minutes),0)!==minutes)throw new Error('Invalid plan');
    return {schedule:schedule.slice(0,20),source:'ai'};
  } catch(e) { console.error('AI plan fallback:',e.message); return {schedule:fallback,source:'local'}; }
}
function validName(name) { return typeof name === 'string' && name.trim().length >= 2 && name.trim().length <= 20; }
function passwordHash(password, salt) { return crypto.scryptSync(password, salt, 64).toString('hex'); }
function validPassword(password) { return typeof password === 'string' && password.length >= 8 && password.length <= 128; }
function requireUser(socket) { const id = socketUsers.get(socket.id); return id && users.get(id); }
app.use(express.static(__dirname));
app.get('/health', (_, res) => res.json({ ok: true, online: onlineList().length, rooms: rooms.size }));
io.on('connection', socket => {
  socket.emit('room_counts', roomCounts());
  socket.emit('room_catalog', [...rooms.values()]);
  sendCommunity(socket); sendLeaderboards(socket);
  socket.on('create_account', ({ name, password } = {}) => {
    if (!validName(name)) return socket.emit('account_error', { message: '昵称需要为 2 到 20 个字符' });
    if (!validPassword(password)) return socket.emit('account_error', { message: '密码需要为 8 到 128 个字符' });
    name = name.trim();
    if ([...users.values()].some(u => u.name.toLocaleLowerCase() === name.toLocaleLowerCase())) return socket.emit('account_error', { message: '这个昵称已被使用，请换一个' });
    let id; do { id = 'ST' + crypto.randomBytes(5).toString('hex').toUpperCase(); } while (users.has(id));
    const salt = crypto.randomBytes(16).toString('hex');
    const user = { id, name, salt, passwordHash: passwordHash(password, salt), createdAt: new Date().toISOString() };
    users.set(id, user); socketUsers.set(socket.id, id); save();
    socket.emit('user_created', safeUser(user)); socket.emit('user_ready', safeUser(user));
    broadcastOnline(); sendLeaderboards(); sendCommunity(socket);
  });
  socket.on('authenticate', ({ id, password } = {}) => {
    const user = users.get(String(id || '').trim().toUpperCase());
    if (!user || !user.salt || !user.passwordHash || !validPassword(password) || passwordHash(password, user.salt) !== user.passwordHash) return socket.emit('auth_error', { message: '账号 ID 或密码错误' });
    socketUsers.set(socket.id, user.id); socket.emit('user_ready', safeUser(user)); broadcastOnline(); sendLeaderboards(); sendCommunity(socket);
  });
  socket.on('request_plan', async ({ goal, minutes, focusMinutes, restMinutes, roomId } = {}) => {
    const user = requireUser(socket); if (!user) return;
    goal = String(goal || '').trim(); minutes = Math.max(15, Math.min(360, Math.round(Number(minutes)||60)));
    if (goal.length < 2 || goal.length > 240) return socket.emit('plan_error', { message: '目标请填写 2 到 240 个字符' });
    const now=Date.now(), previous=planCooldown.get(user.id)||0;
    if(now-previous<30000)return socket.emit('plan_error',{message:'请稍等半分钟再生成下一份计划'});
    planCooldown.set(user.id,now);
    focusMinutes=Math.max(25,Math.min(60,Math.round(Number(focusMinutes)||35)));
    restMinutes=Math.max(1,Math.min(30,Math.round(Number(restMinutes)||5)));
    const result=await createStudyPlan(goal,minutes,focusMinutes,restMinutes);
    if(socket.connected)socket.emit('study_plan',{roomId:String(roomId||''),goal,minutes,...result});
  });
  socket.on('focus_time', ({ seconds } = {}) => {
    const user=requireUser(socket);if(!user)return;const now=Date.now(),last=studyPingAt.get(user.id)||0;
    const amount=Math.max(0,Math.min(60,Math.floor(Number(seconds)||0)));
    if(amount<1|| (last&&now-last<50000))return;
    studyPingAt.set(user.id,now);user.studySeconds=(user.studySeconds||0)+amount;save();sendLeaderboards();
  });
  socket.on('room_focus_progress', ({ seconds } = {}) => {
    const user=requireUser(socket); if(!user)return;
    const amount=Math.max(0,Math.min(60*60*24*30,Math.floor(Number(seconds)||0)));
    for(const [roomId,members] of roomMembers){const member=members.get(socket.id);if(member){member.focusSeconds=amount;broadcastMembers(roomId);}}
  });
  socket.on('focus_time_batch', ({ seconds } = {}) => {
    const user=requireUser(socket); if(!user)return;
    const amount=Math.max(0,Math.min(60*60*24*30,Math.floor(Number(seconds)||0)));
    if(!amount)return socket.emit('focus_time_batch_saved',{seconds:0});
    user.studySeconds=(user.studySeconds||0)+amount;save();sendLeaderboards();
    socket.emit('focus_time_batch_saved',{seconds:amount});
  });
  socket.on('community_post', ({ type, title, body } = {}) => {
    const user=requireUser(socket);if(!user)return;
    type=['buddy','goal','question'].includes(type)?type:'goal';title=String(title||'').trim().slice(0,80);body=String(body||'').trim().slice(0,700);
    if(title.length<2||body.length<2)return socket.emit('community_error',{message:'标题和内容都至少写 2 个字'});
    const post={id:crypto.randomUUID(),type,title,body,authorId:user.id,authorName:user.name,createdAt:new Date().toISOString(),answers:[]};
    communityPosts.push(post);if(communityPosts.length>300)communityPosts.splice(0,communityPosts.length-300);save();io.emit('community_list',communityPosts.slice(-100).reverse().map(publicPost));
  });
  socket.on('community_answer', ({ postId, body } = {}) => {
    const user=requireUser(socket);if(!user)return;body=String(body||'').trim().slice(0,500);if(body.length<2)return socket.emit('community_error',{message:'回答至少写 2 个字'});
    const post=communityPosts.find(p=>p.id===String(postId));if(!post||post.type!=='question')return;
    if(post.answers.some(a=>a.authorId===user.id&&a.body===body))return;
    post.answers.push({id:crypto.randomUUID(),authorId:user.id,authorName:user.name,body,createdAt:new Date().toISOString()});
    user.helpCount=(user.helpCount||0)+1;save();io.emit('community_list',communityPosts.slice(-100).reverse().map(publicPost));sendLeaderboards();
  });
  socket.on('change_name', ({ name } = {}) => {
    const user = requireUser(socket); if (!user || !validName(name)) return socket.emit('account_error', { message: '昵称需要为 2 到 20 个字符' });
    name = name.trim();
    if ([...users.values()].some(u => u.id !== user.id && u.name.toLocaleLowerCase() === name.toLocaleLowerCase())) return socket.emit('account_error', { message: '这个昵称已被使用，请换一个' });
    user.name = name; save(); socket.emit('name_updated', safeUser(user));
    for (const members of roomMembers.values()) { const member = members.get(socket.id); if (member) member.name = name; }
    for (const roomId of roomMembers.keys()) broadcastMembers(roomId);
    broadcastOnline();
  });
  socket.on('create_room', ({ room } = {}) => {
    const user = requireUser(socket); if (!user || !room || !room.name) return;
    const id = String(room.id || Date.now());
    const saved = { id, name: String(room.name).slice(0, 60), type: String(room.type || '自习室'), icon: String(room.icon || '📚'), desc: String(room.desc || '欢迎加入自习。').slice(0, 180), close: String(room.close || 'none'), people: 0, custom: true, ownerId: user.id, ownerName: user.name, code: String(room.code || id).slice(-6).padStart(6, '0') };
    rooms.set(id, saved); save(); catalog(); socket.emit('room_created', saved);
  });
  socket.on('join_room', data => {
    const user = requireUser(socket); if (!user || !data || !data.roomId) return;
    const roomId = String(data.roomId);
    for (const [rid, members] of roomMembers) { if (members.delete(socket.id)) { socket.leave('room:' + rid); broadcastMembers(rid); } }
    socket.join('room:' + roomId);
    if (!roomMembers.has(roomId)) roomMembers.set(roomId, new Map());
    roomMembers.get(roomId).set(socket.id, { id: user.id, name: user.name, avatar: '🙂', status: ['休息中', '学习中'].includes(data.status) ? data.status : '学习中', focusSeconds: 0 });
    socket.emit('room_counts', roomCounts()); broadcastMembers(roomId);
  });
  socket.on('leave_room', data => { const rid = String(data && data.roomId || ''); const members = roomMembers.get(rid); if (members && members.delete(socket.id)) { socket.leave('room:' + rid); broadcastMembers(rid); } });
  socket.on('room_message', data => {
    const user = requireUser(socket); const roomId = String(data && data.roomId || ''); const text = String(data && data.text || '').trim();
    if (!user || !roomMembers.get(roomId)?.has(socket.id) || text.length < 2 || text.length > 180) return;
    io.to('room:' + roomId).emit('room_message', { roomId, name: user.name, text, time: new Date().toLocaleTimeString('zh-CN', { hour: '2-digit', minute: '2-digit' }) });
  });
  socket.on('update_status', data => { const rid = String(data && data.roomId || ''); const m = roomMembers.get(rid)?.get(socket.id); if (m && ['学习中', '休息中'].includes(data.status)) { m.status = data.status; broadcastMembers(rid); } });
  socket.on('room_reaction', data => { const user = requireUser(socket); const rid = String(data && data.roomId || ''); if (user && roomMembers.get(rid)?.has(socket.id)) io.to('room:' + rid).emit('room_reaction', { roomId: rid, name: user.name, text: '👏 给大家加油！' }); });
  socket.on('search_user', ({ query } = {}) => {
    const q = String(query || '').trim().toLocaleLowerCase(); if (!q) return socket.emit('friend_result', { user: null });
    const found = [...users.values()].find(u => u.id.toLocaleLowerCase() === q || u.name.toLocaleLowerCase() === q);
    const onlineIds = new Set(socketUsers.values()); socket.emit('friend_result', { user: found ? { ...safeUser(found), online: onlineIds.has(found.id) } : null });
  });
  socket.on('invite_user', ({ userId, roomId } = {}) => {
    const from = requireUser(socket); const room = rooms.get(String(roomId)); if (!from || !room) return;
    let sent = false;
    for (const [sid, uid] of socketUsers) if (uid === String(userId)) { io.to(sid).emit('invite_received', { fromName: from.name, roomId: String(roomId), roomName: room.name }); sent = true; }
    socket.emit('invite_sent', { message: sent ? '邀请已发送' : '好友当前不在线' });
  });
  socket.on('disconnect', () => {
    const disconnectedUserId=socketUsers.get(socket.id);socketUsers.delete(socket.id);if(disconnectedUserId)studyPingAt.delete(disconnectedUserId);
    for (const [rid, members] of roomMembers) if (members.delete(socket.id)) broadcastMembers(rid);
    broadcastOnline();
  });
});
server.listen(PORT, '0.0.0.0', () => console.log(`Study Together running at http://localhost:${PORT}`));

