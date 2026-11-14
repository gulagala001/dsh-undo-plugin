import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtemp,mkdir,writeFile,readFile,rm,symlink,readlink} from 'node:fs/promises';
import {join} from 'node:path';
import {tmpdir} from 'node:os';
import {Session} from '@deepseek-ai/dsh-session';
import {createUserMessage,createMessage} from '@deepseek-ai/dsh-llm';
import {hiddenNode, visibleGroup} from '../src/client/visibility.mjs';
import {inputBoundaries} from '../src/boundary.mjs';
import {RewindEngine} from '../src/engine.mjs';
import {snapshot,prepareReplay,appendPrepared,validateHistory} from '../src/conversation.mjs';
const messages=s=>s.deriveMessages().map(({id,...m})=>m);
async function fixture(t){const dir=await mkdtemp(join(tmpdir(),'rewind-'));t.after(()=>rm(dir,{recursive:true,force:true}));const cwd=join(dir,'project');await mkdir(cwd);const session=Session.create('same-session',undefined,{...Session.create('same-session').header,cwd});const agent={id:session.id,session,status:'idle',runMaintenance:fn=>fn()};const ctx={sessions:{get:()=>session,flush:async()=>true,messageProjections:[]},agents:{get:()=>agent,list:()=>[agent]}};const engine=new RewindEngine(ctx,join(dir,'store'));agent.inbox={nextTurn:[],nextStep:[],prepend(target,m){this[target==='next-turn'?'nextTurn':'nextStep'].unshift(m);}};agent.steer=m=>queueMicrotask(async()=>{const turn=(session.snapshotEvents().findLast(e=>e.type==='turn/start')?.data.turn||0)+1;session.append('turn/start',{turn});await engine.preStep({agent,messages:[m],turn,step:1},()=>{throw Error('maintenance requested model');});const end=session.append('turn/end',{turn,reason:{kind:'blocked'}});engine.finish(session,end);});return {dir,cwd,session,agent,engine,ctx};}
async function user(f,text,turn){const m=createUserMessage({source:{kind:'user'},content:[{type:'text',text}]});f.session.append('turn/start',{turn});await f.engine.checkpoint(f.agent,[m]);const e=f.session.append('user/message',m,{surfaceOp:'append'});f.session.append('step/start',{turn,step:1});return e.seq;}
function finish(f,text,turn){f.session.append('assistant/message',{turn,step:1,stream:[],message:createMessage({role:'assistant',source:{kind:'model',provider:'test',model:'test'},content:[{type:'text',text}]})},{surfaceOp:'append'});f.session.append('step/end',{turn,step:1});f.session.append('turn/end',{turn,reason:{kind:'completed'}});}
const todo = s => s.snapshotEvents().findLast(e => e.type === 'todo/write')?.data;
const taskRecord = title => ({ todos: [{ content: title, status: 'pending' }],
 tasks: [{ id: 'T4', title, done: false, anchor: null, links: [{ id: 'L2', kind: 'text', note: 'observed', reason: 'manual' }] }],
 excerpts: [], nextT: 5, nextE: 1, nextL: 3, extension: { preserved: true } });

test('rewind and undo restore the complete native task record and cold history', async t => {
 const f = await fixture(t), earlier = taskRecord('earlier'), later = taskRecord('later');
 await user(f, 'first request', 1); f.session.append('todo/write', earlier); finish(f, 'first result', 1);
 const target = await user(f, 'second request', 2); f.session.append('todo/write', later); finish(f, 'second result', 2);
 const original = structuredClone(f.session.snapshotEvents());
 const plan = await f.engine.preview(f.session.id, target); assert.equal(plan.canApply, true, plan.reason);
 await f.engine.execute(f.session.id, plan.planId);
 assert.deepEqual(todo(f.session), earlier);
 const restored = Session.create(f.session.id, f.session.snapshotEvents(), f.session.header);
 assert.deepEqual(todo(restored), earlier);
 const undo = await f.engine.preview(f.session.id, null, true); assert.equal(undo.canApply, true, undo.reason);
 await f.engine.execute(f.session.id, undo.planId);
 assert.deepEqual(todo(f.session), later);
 assert.deepEqual(f.session.snapshotEvents().slice(0, original.length), original);
 validateHistory(f.session);
});

test('rewind before the first task writes an empty native snapshot and undo restores evidence', async t => {
 const f = await fixture(t), target = await user(f, 'first request', 1), record = taskRecord('new task');
 f.session.append('todo/write', record); finish(f, 'result', 1);
 const plan = await f.engine.preview(f.session.id, target); await f.engine.execute(f.session.id, plan.planId);
 assert.deepEqual(todo(f.session), { todos: [] });
 const undo = await f.engine.preview(f.session.id, null, true); await f.engine.execute(f.session.id, undo.planId);
 assert.deepEqual(todo(f.session), record);
});

test('failed replay publication restores the pre-operation task record through the same recovery path', async t => {
 const f = await fixture(t); f.session.append('todo/write', taskRecord('before request'));
 const target = await user(f, 'change tasks', 1), record = taskRecord('before failed rewind');
 f.session.append('todo/write', record); finish(f, 'result', 1);
 const plan = await f.engine.preview(f.session.id, target); let flushes = 0;
 // Fail before the commit marker, after the task snapshot has been appended.
 const append = f.session.append.bind(f.session); let fail = true;
 f.session.append = (type, ...args) => {
  if (type === 'plan/mode' && fail) { fail = false; throw Error('synthetic commit failure'); }
  return append(type, ...args);
 };
 f.ctx.sessions.flush = async () => { flushes++; return true; };
 await assert.rejects(f.engine.execute(f.session.id, plan.planId), /synthetic commit failure/);
 assert.deepEqual(todo(f.session), record);
 assert.equal((await f.engine.store.load(f.session.id)).journal, null);
 assert.ok(flushes); validateHistory(f.session);
});

test('legacy message-only replay does not invent a task cutoff', async t => {
 const f = await fixture(t), target = await user(f, 'request', 1); finish(f, 'answer', 1);
 const record = taskRecord('legacy current task'); f.session.append('todo/write', record);
 const oldContext = snapshot(f.session), plan = await f.engine.preview(f.session.id, target);
 await f.engine.execute(f.session.id, plan.planId);
 const st = await f.engine.store.load(f.session.id); st.undo.at(-1).context = oldContext; await f.engine.store.save(st);
 f.session.append('todo/write', record);
 const undo = await f.engine.preview(f.session.id, null, true);
 assert.ok(undo.warnings.some(warning => warning.includes('任务清单保持现状')));
 await f.engine.execute(f.session.id, undo.planId);
 assert.deepEqual(todo(f.session), record);
});
test('plain chat records a checkpoint without scanning files and backup failures do not prevent tools',async t=>{
 const f=await fixture(t);let captures=0;
 f.engine.files.capture=async()=>{captures++;throw Error('synthetic backup unavailable');};
 const seq=await user(f,'chat',1);assert.equal(captures,0);
 let ran=false;await f.engine.tool({agent:f.agent},async()=>{ran=true;});assert.equal(ran,true);assert.equal(captures,1,'a failed pre-image must not trigger another full scan after the tool');
 finish(f,'done',1);const plan=await f.engine.preview(f.session.id,seq);assert.equal(plan.canApply,true);assert.match(plan.warnings.join(' '),/未完整备份/);await f.engine.execute(f.session.id,plan.planId);assert.equal(messages(f.session).length,0);
});

test('snapshot timeout lets concurrent tools run, records the gap and avoids immediate rescans',async t=>{
 const f=await fixture(t),seq=await user(f,'large workspace',1);
 f.engine.root=async()=>f.cwd;
 const entered=Promise.withResolvers();let captures=0;
 f.engine.files.capture=async(_cwd,{signal})=>{
   captures++;entered.resolve();
   return new Promise((_,reject)=>signal.addEventListener('abort',()=>reject(signal.reason),{once:true}));
 };
 t.mock.timers.enable({apis:['setTimeout']});
 const first=f.engine.tool({agent:f.agent,name:'run_code'},()=>42);
 await entered.promise;
 const second=f.engine.tool({agent:f.agent,name:'run_code'},()=>43);
 t.mock.timers.tick(4000);
 assert.deepEqual(await Promise.all([first,second]),[42,43]);
 assert.equal(captures,1);
 assert.deepEqual(await f.engine.tool({agent:f.agent,name:'run_code'},()=>44),44);
 assert.equal(captures,1);
 finish(f,'done',1);
 const plan=await f.engine.preview(f.session.id,seq);
 assert.equal(plan.canApply,true);assert.match(plan.warnings.join(' '),/超出时间预算/);await f.engine.execute(f.session.id,plan.planId);assert.equal(messages(f.session).length,0);
 assert.equal(f.engine.waves.size,0);
});

for(const phase of ['before','after'])test(`cancel during the ${phase} snapshot releases the workspace and leaves only real backup gaps`,async t=>{
 const f=await fixture(t),seq=await user(f,'stop during backup',1),controller=new AbortController();
 const entered=Promise.withResolvers(),realCapture=f.engine.files.capture.bind(f.engine.files);
 let captures=0,bodies=0;
 f.engine.files.capture=async(cwd,options)=>{
   if(++captures!==(phase==='before'?1:2))return realCapture(cwd,options);
   entered.resolve();
   return new Promise((_,reject)=>options.signal.addEventListener('abort',()=>reject(options.signal.reason),{once:true}));
 };
 const result=f.engine.tool({agent:f.agent,signal:controller.signal},()=>++bodies).catch(error=>error);
 await entered.promise;controller.abort(Error('stop snapshot'));
 assert.equal(phase==='before'?(await result).message:await result,phase==='before'?'stop snapshot':1);
 assert.equal(bodies,phase==='before'?0:1);
 assert.equal(f.engine.waves.size,0);
 assert.deepEqual(await f.engine.tool({agent:f.agent},()=>++bodies),phase==='before'?1:2);
 finish(f,'done',1);
 const plan=await f.engine.preview(f.session.id,seq);
 assert.equal(plan.canApply,true);
 if(phase==='after')assert.match(plan.warnings.join(' '),/工具执行已取消/);
 await f.engine.execute(f.session.id,plan.planId);assert.equal(messages(f.session).length,0);
});
test('cold history can read rewind state without activating an agent, then preview resumes it',async t=>{
 const f=await fixture(t),seq=await user(f,'cold request',1);finish(f,'done',1);
 const st=await f.engine.store.load(f.session.id);st.hiddenRanges=[{start:seq,end:seq}];await f.engine.store.save(st);
 let active=false,resumes=0;
 f.ctx.sessions.get=()=>active?f.session:undefined;f.ctx.agents.get=()=>active?f.agent:undefined;
 f.ctx.sessionQuery={observeSession:async()=>({header:f.session.header,events:f.session.snapshotEvents(),inheritedEventCount:0,[Symbol.dispose](){}})};f.ctx.sessionController={resolveAgent:async()=>{resumes++;active=true;return {agent:f.agent};}};
 const state=await f.engine.getState(f.session.id);assert.equal(state.error,undefined);assert.deepEqual(state.hiddenTurns,[1]);assert.equal(resumes,0);
 st.hiddenRanges=[];await f.engine.store.save(st);
 const plan=await f.engine.preview(f.session.id,seq);assert.equal(plan.canApply,true,plan.reason);assert.equal(resumes,1);
});
test('same session rewind and undo restore text, new binary files and original append-only history',async t=>{
 const f=await fixture(t),file=join(f.cwd,'a');await writeFile(file,'A');const seq=await user(f,'change',1);
 await f.engine.tool({agent:f.agent},async()=>{await writeFile(file,'B');});await f.engine.tool({agent:f.agent},async()=>{await writeFile(file,'C');await writeFile(join(f.cwd,'new.bin'),Buffer.from([0,255,128,1]));});finish(f,'done',1);
 const original=structuredClone(f.session.snapshotEvents()),before=messages(f.session),p=await f.engine.preview(f.session.id,seq);assert.equal(p.canApply,true,p.reason);assert.equal(p.files.length,2);
 const result=await f.engine.execute(f.session.id,p.planId,{text:'draft',attachmentIds:[]});assert.equal(await readFile(file,'utf8'),'A');await assert.rejects(readFile(join(f.cwd,'new.bin')));assert.equal(f.session.id,'same-session');assert.equal(f.session.deriveMessages().length,0);assert.equal(result.state.undoAvailable,true);assert.deepEqual(f.session.snapshotEvents().slice(0,original.length),original);
 const undo=await f.engine.preview(f.session.id,null,true);assert.equal(undo.canApply,true,undo.reason);await f.engine.execute(f.session.id,undo.planId);assert.equal(await readFile(file,'utf8'),'C');assert.deepEqual(await readFile(join(f.cwd,'new.bin')),Buffer.from([0,255,128,1]));assert.deepEqual(messages(f.session),before);
 const cold=Session.create(f.session.id,f.session.snapshotEvents(),f.session.header);assert.deepEqual(messages(cold),before);assert.ok((await f.engine.getState(f.session.id)).hiddenRanges.some(r=>r.start===original.length));
});
test('picker reads cold and compacted history in bounded pages, omitting withdrawn and replayed messages',async t=>{
 const f=await fixture(t),seqs=[];
 for(let i=0;i<105;i++){seqs.push(await user(f,'request '+i+' '+ 'x'.repeat(1000),i+1));finish(f,'answer',i+1);}
 const nodes=[...f.session.surface.nodes];f.session.append('user/message',createUserMessage({source:{kind:'test-summary'},content:[{type:'text',text:'summary'}]}),{surfaceOp:{op:'replace',startSeq:nodes[0],endSeq:nodes.at(-1)},sourceEventSeqs:nodes});
 const st=await f.engine.store.load(f.session.id);st.hiddenRanges=[{start:seqs.at(-1),end:seqs.at(-1)}];await f.engine.store.save(st);
 f.ctx.sessions.get=()=>undefined;f.ctx.agents.get=()=>{throw Error('picker must not activate agents');};
 f.ctx.sessionQuery={observeSession:async()=>({header:f.session.header,events:f.session.snapshotEvents(),inheritedEventCount:0,[Symbol.dispose](){}})};f.ctx.sessionController={};f.engine.files.capture=()=>{throw Error('picker must not scan files');};
 const first=await f.engine.candidates(f.session.id);assert.equal(first.items.length,100);assert.equal(first.items[0].seq,seqs[103]);assert.ok(first.items.every(i=>i.text.length<=240));assert.equal(first.before,seqs[4]);
 const second=await f.engine.candidates(f.session.id,first.before);assert.deepEqual(second.items.map(i=>i.seq),seqs.slice(0,4).reverse());assert.ok(second.items.every(i=>i.conversationOnly));assert.equal(second.before,null);
 await assert.rejects(f.engine.candidates(f.session.id,-1),/无效/);
});
test('manual edits and stale confirmation do not get overwritten',async t=>{const f=await fixture(t);await writeFile(join(f.cwd,'a'),'A');const seq=await user(f,'edit',1);await f.engine.tool({agent:f.agent},()=>writeFile(join(f.cwd,'a'),'B'));finish(f,'done',1);const p=await f.engine.preview(f.session.id,seq);await writeFile(join(f.cwd,'a'),'manual');await f.engine.execute(f.session.id,p.planId);assert.equal(await readFile(join(f.cwd,'a'),'utf8'),'manual');assert.equal(f.session.deriveMessages().length,0);const undo=await f.engine.preview(f.session.id,null,true);await f.engine.execute(f.session.id,undo.planId);assert.equal(await readFile(join(f.cwd,'a'),'utf8'),'manual');assert.equal(messages(f.session).length,2);});
test('restore failure keeps only successful file changes in undo and still rewinds conversation',async t=>{const f=await fixture(t);await writeFile(join(f.cwd,'a'),'A');await writeFile(join(f.cwd,'b'),'A');const seq=await user(f,'edit',1);await f.engine.tool({agent:f.agent},async()=>{await writeFile(join(f.cwd,'a'),'B');await writeFile(join(f.cwd,'b'),'B');});finish(f,'done',1);const p=await f.engine.preview(f.session.id,seq);const put=f.engine.files.put.bind(f.engine.files);let fail=true;f.engine.files.put=async(root,path,value,expected)=>{if(path==='b'&&fail){fail=false;throw Error('injected write failure');}return put(root,path,value,expected);};await f.engine.execute(f.session.id,p.planId);assert.equal(await readFile(join(f.cwd,'a'),'utf8'),'A');assert.equal(await readFile(join(f.cwd,'b'),'utf8'),'B');assert.equal(f.session.deriveMessages().length,0);assert.equal((await f.engine.store.load(f.session.id)).journal,null);const undo=await f.engine.preview(f.session.id,null,true);assert.deepEqual(undo.files.map(f=>f.path),['a']);await f.engine.execute(f.session.id,undo.planId);assert.equal(await readFile(join(f.cwd,'a'),'utf8'),'B');assert.equal(await readFile(join(f.cwd,'b'),'utf8'),'B');});
test('tool-call pairing survives native surface replay',async t=>{const f=await fixture(t);await user(f,'tool',1);const call={type:'tool-call',id:'original-call',name:'read',arguments:'{}'};f.session.append('assistant/message',{turn:1,step:1,stream:[],message:createMessage({role:'assistant',source:{kind:'model',provider:'test',model:'test'},content:[call]})},{surfaceOp:'append'});f.session.append('tool/call',{turn:1,step:1,callId:'original-call',name:'read',arguments:'{}'});f.session.append('tool/result',{turn:1,step:1,message:{id:'result',role:'tool',source:{kind:'tool',name:'read',callId:'original-call'},toolCallId:'original-call',content:[{type:'text',text:'ok'}]}},{surfaceOp:'append'});f.session.append('step/end',{turn:1,step:1});f.session.append('turn/end',{turn:1,reason:{kind:'completed'}});const context=snapshot(f.session);appendPrepared(f.session,prepareReplay(f.session,[]));appendPrepared(f.session,prepareReplay(f.session,context));const ms=f.session.deriveMessages();assert.equal(ms[1].content[0].id,ms[2].toolCallId);assert.notEqual(ms[2].toolCallId,'original-call');assert.equal(f.session.snapshotEvents().filter(e=>e.type==='turn/start').length,3);Session.create(f.session.id,f.session.snapshotEvents(),f.session.header);validateHistory(f.session);});
test('rewind crosses a compacted surface and undo restores that exact summary',async t=>{
 const f=await fixture(t),seq=await user(f,'old request',1);finish(f,'old answer',1);
 const nodes=[...f.session.surface.nodes];f.session.append('user/message',createUserMessage({source:{kind:'test-summary'},content:[{type:'text',text:'compacted summary'}]}),{surfaceOp:{op:'replace',startSeq:nodes[0],endSeq:nodes.at(-1)},sourceEventSeqs:nodes});
 const before=messages(f.session),plan=await f.engine.preview(f.session.id,seq);await f.engine.execute(f.session.id,plan.planId);
 const undo=await f.engine.preview(f.session.id,null,true);await f.engine.execute(f.session.id,undo.planId);assert.deepEqual(messages(f.session),before);validateHistory(f.session);
});
test('child-agent tool changes belong to the parent checkpoint',async t=>{
 const f=await fixture(t);await writeFile(join(f.cwd,'a'),'A');const seq=await user(f,'delegate',1);
 const childSession=Session.create('child',undefined,{...Session.create('child').header,cwd:f.cwd,origin:'subagent',parentSession:f.agent.id});
 const child={id:'child',session:childSession};await f.engine.tool({agent:child},()=>writeFile(join(f.cwd,'a'),'child'));finish(f,'done',1);
 const plan=await f.engine.preview(f.session.id,seq);assert.equal(plan.files.length,1);await f.engine.execute(f.session.id,plan.planId);assert.equal(await readFile(join(f.cwd,'a'),'utf8'),'A');
});
test('child changes outside the parent workspace cannot claim complete restoration',async t=>{
 const f=await fixture(t),other=join(f.dir,'other');await mkdir(other);
 const seq=await user(f,'delegate elsewhere',1);
 const child={id:'child',session:Session.create('child',undefined,{...Session.create('child').header,cwd:other,origin:'subagent',parentSession:f.agent.id})};
 await f.engine.tool({agent:child},()=>writeFile(join(other,'a'),'outside'));finish(f,'done',1);
 const plan=await f.engine.preview(f.session.id,seq);assert.equal(plan.canApply,true);assert.match(plan.warnings.join(' '),/跨工作目录/);await f.engine.execute(f.session.id,plan.planId);assert.equal(messages(f.session).length,0);
 assert.equal(await readFile(join(other,'a'),'utf8'),'outside');
});
test('overlapping sessions are recorded as ambiguous and never overwrite each other',async t=>{
 const f=await fixture(t);await writeFile(join(f.cwd,'a'),'A');const seq=await user(f,'parallel',1);
 const other={id:'other',session:Session.create('other',undefined,{...Session.create('other').header,cwd:f.cwd})};let release,started;const began=new Promise(r=>started=r),wait=new Promise(r=>release=r);
 const first=f.engine.tool({agent:f.agent},async()=>{started();await wait;await writeFile(join(f.cwd,'a'),'first');});await began;
 await f.engine.tool({agent:other},()=>writeFile(join(f.cwd,'b'),'other'));release();await first;finish(f,'done',1);
 const plan=await f.engine.preview(f.session.id,seq);assert.equal(plan.canApply,true);assert.match(plan.warnings.join(' '),/其他会话/);await f.engine.execute(f.session.id,plan.planId);assert.equal(messages(f.session).length,0);assert.equal(await readFile(join(f.cwd,'b'),'utf8'),'other');
});
test('interrupted file transaction is recovered from disk before another mutation',async t=>{
 const f=await fixture(t);await writeFile(join(f.cwd,'a'),'A');await user(f,'edit',1);await f.engine.tool({agent:f.agent},()=>writeFile(join(f.cwd,'a'),'B'));finish(f,'done',1);
 const st=await f.engine.store.load(f.session.id),context=snapshot(f.session),record=st.changes[0];st.journal={id:'interrupted',nativeStart:f.session.seq,root:record.root,changes:record.changes,context,metadata:f.engine.metadata(f.session)};await f.engine.store.save(st);
 await f.engine.files.apply(record.root,record.changes);
 const {Store}=await import('../src/store.mjs'),{Files}=await import('../src/files.mjs');f.engine.store=new Store(join(f.dir,'store'));f.engine.files=new Files(join(f.dir,'store'));
 await f.engine.launch(f.session.id,{recovery:true});assert.equal(await readFile(join(f.cwd,'a'),'utf8'),'B');assert.equal((await f.engine.store.load(f.session.id)).journal,null);validateHistory(f.session);
});

test('a conflict during restoration preserves the new edit through rewind and undo',async t=>{
 const f=await fixture(t),a=join(f.cwd,'a'),b=join(f.cwd,'b');await writeFile(a,'A1');await writeFile(b,'A2');const seq=await user(f,'edit',1);
 await f.engine.tool({agent:f.agent},async()=>{await writeFile(a,'B1');await writeFile(b,'B2');});finish(f,'done',1);
 const plan=await f.engine.preview(f.session.id,seq),before=messages(f.session),readBlob=f.engine.files.readBlob.bind(f.engine.files);
 f.engine.files.readBlob=async key=>{const bytes=await readBlob(key);if(bytes.toString()==='A2')await writeFile(b,'external edit');return bytes;};
 await f.engine.execute(f.session.id,plan.planId);
 assert.equal(await readFile(a,'utf8'),'A1');assert.equal(await readFile(b,'utf8'),'external edit');assert.equal(messages(f.session).length,0);
 assert.equal((await f.engine.store.load(f.session.id)).journal,null);
 const {Store}=await import('../src/store.mjs'),{Files}=await import('../src/files.mjs');
 f.engine.store=new Store(join(f.dir,'store'));f.engine.files=new Files(join(f.dir,'store'));
 const undo=await f.engine.preview(f.session.id,null,true);await f.engine.execute(f.session.id,undo.planId);
 assert.equal(await readFile(a,'utf8'),'B1');assert.equal(await readFile(b,'utf8'),'external edit');assert.equal((await f.engine.store.load(f.session.id)).journal,null);
 assert.deepEqual(messages(f.session),before);validateHistory(f.session);
});

test('replay appends use the native sequence fence without copying the entire log per event',async t=>{
 const f=await fixture(t);await user(f,'request',1);finish(f,'answer',1);
 const replay=prepareReplay(f.session,[]),snapshotEvents=f.session.snapshotEvents.bind(f.session);let copies=0;
 f.session.snapshotEvents=(...args)=>{copies++;return snapshotEvents(...args);};
 appendPrepared(f.session,replay);assert.equal(copies,0);validateHistory(f.session);
 const stale=prepareReplay(f.session,[]);f.session.append('plan/mode',{active:true});
 assert.throws(()=>appendPrepared(f.session,stale),/会话在确认后发生变化/);
});


async function admittedInput(f, text, turn, step = 1, openTurn = true) {
 const message = createUserMessage({source:{kind:'user'},content:[{type:'text',text}]});
 if(openTurn)f.session.append('turn/start',{turn});
 await f.engine.checkpoint(f.agent,[message]);
 const start=f.session.append('step/start',{turn,step}).seq;
 const injection=f.session.append('user/message',createUserMessage({source:{kind:'plugin:oh-my-dsh:tasks'},content:[{type:'text',text:'[runtime state] synthetic'}]}),{surfaceOp:'append'}).seq;
 const seq=f.session.append('user/message',message,{surfaceOp:'append'}).seq;
 return {start,injection,seq};
}

test('rewind removes the native pre-input prologue and its process group; undo restores it exactly',async t=>{
 const f=await fixture(t),{start,injection,seq}=await admittedInput(f,'request',1);
 finish(f,'answer',1);
 const original=structuredClone(f.session.snapshotEvents()),before=messages(f.session);
 const plan=await f.engine.preview(f.session.id,seq),result=await f.engine.execute(f.session.id,plan.planId);
 assert.equal(messages(f.session).length,0,'runtime prologue must not remain in model context');
 assert.deepEqual(result.state.hiddenTurns,[1,2]);
 assert.equal(result.state.hiddenSteps,2);
 const location={kind:'step',turn:{turn:1},step:{start:{seq:start}}};
 const nodes=new Map([
  ['injection',{anchorSeq:injection,data:{seq:injection},location}],
  ['prompt',{anchorSeq:start-0.1,data:{text:'prompt'},location}],
  ['process',{anchorSeq:start-0.1,data:{turn:1},location:{kind:'turn',turn:{turn:1}}}],
 ]);
 for(const node of nodes.values())assert.equal(hiddenNode(node,result.state),true);
 assert.equal(visibleGroup({members:[{key:'injection'}],data:{turn:1}},nodes,result.state),undefined);
 const cold=Session.create(f.session.id,f.session.snapshotEvents(),f.session.header);
 assert.equal(messages(cold).length,0);
 const undo=await f.engine.preview(f.session.id,null,true);await f.engine.execute(f.session.id,undo.planId);
 assert.deepEqual(messages(f.session),before);
 const state=await f.engine.getState(f.session.id);
 for(const node of nodes.values())assert.equal(hiddenNode(node,state),false);
 assert.deepEqual(f.session.snapshotEvents().slice(0,original.length),original);
 validateHistory(f.session);
});

test('rewind of later-step steering removes only that admission and keeps earlier work in its turn',async t=>{
 const f=await fixture(t);await admittedInput(f,'first',1);
 f.session.append('assistant/message',{turn:1,step:1,stream:[],message:createMessage({role:'assistant',source:{kind:'model',provider:'test',model:'test'},content:[{type:'text',text:'earlier work'}]})},{surfaceOp:'append'});
 f.session.append('step/end',{turn:1,step:1});
 const before=messages(f.session),{start,injection,seq}=await admittedInput(f,'steering',1,2,false);
 f.session.append('step/end',{turn:1,step:2});f.session.append('turn/end',{turn:1,reason:{kind:'completed'}});
 const plan=await f.engine.preview(f.session.id,seq),result=await f.engine.execute(f.session.id,plan.planId);
 assert.deepEqual(messages(f.session),before);
 assert.equal(result.state.hiddenTurns.includes(1),false);
 assert.equal(result.state.hiddenSteps,2,'one removed step and one maintenance step');
 assert.equal(hiddenNode({anchorSeq:start-0.1,data:{},location:{kind:'step',turn:{turn:1},step:{start:{seq:start}}}},result.state),true);
 const kept={anchorSeq:1,data:{seq:1},location:{kind:'step',turn:{turn:1},step:{start:{seq:1}}}};
 assert.equal(hiddenNode(kept,result.state),false);
 const group={members:[{key:'kept'},{key:'injection'}],data:{turn:1}};
 assert.deepEqual(visibleGroup(group,new Map([['kept',kept],['injection',{anchorSeq:injection}]]),result.state).members,[{key:'kept'}]);
});

test('second input in the same admission preserves the first input and shared prologue',async t=>{
 const f=await fixture(t);await admittedInput(f,'first queued input',1);
 const before=messages(f.session),m=createUserMessage({source:{kind:'user'},content:[{type:'text',text:'second queued input'}]});
 await f.engine.checkpoint(f.agent,[m]);const seq=f.session.append('user/message',m,{surfaceOp:'append'}).seq;finish(f,'answer',1);
 const plan=await f.engine.preview(f.session.id,seq),result=await f.engine.execute(f.session.id,plan.planId);
 assert.deepEqual(messages(f.session),before);
 assert.equal(result.state.hiddenTurns.includes(1),false);
 assert.equal(result.state.hiddenSteps,1,'only the maintenance step is hidden');
});

test('legacy rewind ranges hide pre-input residues on cold reads without rewriting history or undo data',async t=>{
 const f=await fixture(t),{start,injection,seq}=await admittedInput(f,'request',1);finish(f,'answer',1);
 const st=await f.engine.store.load(f.session.id);st.hiddenRanges=[{start:seq,end:f.session.seq-1}];
 await f.engine.store.save(st);const original=structuredClone(f.session.snapshotEvents());
 f.ctx.sessions.get=()=>undefined;
 f.ctx.sessionQuery={observeSession:async()=>({header:f.session.header,events:original,inheritedEventCount:0,[Symbol.dispose](){}})};
 const state=await f.engine.getState(f.session.id);
 assert.equal(state.hiddenRanges[0].start,0);assert.equal(state.hiddenSteps,1);
 assert.equal(hiddenNode({anchorSeq:injection,data:{seq:injection}},state),true);
 assert.equal(hiddenNode({anchorSeq:start-0.1,location:{kind:'step',turn:{turn:1},step:{start:{seq:start}}}},state),true);
 assert.deepEqual((await f.engine.store.load(f.session.id)).hiddenRanges,st.hiddenRanges);
 assert.deepEqual(f.session.snapshotEvents(),original);
});

test('input boundary never crosses prior output or tool side effects in the same step',()=>{
 for(const type of ['assistant/message','assistant/live-chunk','tool/call','tool/result']){
  const events=[{seq:0,type:'turn/start',data:{turn:1}},{seq:1,type:'step/start',data:{turn:1,step:1}},
   {seq:2,type,data:{}},{seq:3,type:'user/message',data:{source:{kind:'user'}}}];
  assert.equal(inputBoundaries(events).get(3),3,type);
 }
});

for (const replacement of ['prefix', 'middle', 'system-head']) test(`surface replay follows positions after ${replacement} replacement`, async t => {
 const f=await fixture(t),s=f.session;
 s.append('turn/start',{turn:1});s.append('step/start',{turn:1,step:1});
 const role=replacement==='system-head'?'system':'developer';
 const first=s.append(role+'/message',{turn:1,step:1,message:createMessage({role,source:{kind:'system-prompt'},content:[{type:'text',text:'retained prefix'}]})},{surfaceOp:'append'}).seq;
 const middle=s.append('user/message',createUserMessage({source:{kind:'user'},content:[{type:'text',text:'earlier input'}]}),{surfaceOp:'append'}).seq;
 s.append('developer/message',{turn:1,step:1,message:createMessage({role:'developer',source:{kind:'test-empty'},content:[]})},{surfaceOp:'append'});
 const tail=s.append('user/message',createUserMessage({source:{kind:'user'},content:[{type:'text',text:'later input'}]}),{surfaceOp:'append'}).seq;
 const replaced=replacement==='middle'?middle:first,old=s.eventAt(replaced);
 const newest=s.append(old.type,structuredClone(old.data),{surfaceOp:{op:'replace',startSeq:replaced,endSeq:replaced},sourceEventSeqs:[replaced]}).seq;
 finish(f,'later answer',1);
 const original=structuredClone(s.snapshotEvents()),before=messages(s),desired=snapshot(s).slice(0,replacement==='middle'?2:1);
 assert.equal(desired.at(-1).source,newest);assert.ok(newest>tail);
 appendPrepared(s,prepareReplay(s,desired));
 assert.deepEqual(messages(s),desired.map(item=>{const{id,...m}=item.type==='user/message'?item.data:item.data.message;return m;}));
 assert.deepEqual(s.snapshotEvents().slice(0,original.length),original);
 validateHistory(s);
 const cold=Session.create(s.id,s.snapshotEvents(),s.header);
 appendPrepared(cold,prepareReplay(cold,{atSeq:original.at(-1).seq+1}));
 assert.deepEqual(messages(cold),before,'undo/recovery restores the exact nonmonotonic surface');
 validateHistory(cold);
});

test('unclaimed markers retire without consuming unrelated native input', async t => {
  const f=await fixture(t), marker=Promise.withResolvers();
  f.agent.steer=m=>marker.resolve(m);
  const pending=f.engine.launch(f.session.id,{recovery:true});
  const rejected=assert.rejects(pending,/丢弃/); await marker.promise;
  const unrelated=createUserMessage({source:{kind:'user'},content:[{type:'text',text:'continue'}]});
  const decision=await f.engine.preStep({agent:f.agent,messages:[unrelated],turn:1},()=>({kind:'enter',messages:[unrelated]}));
  assert.equal(decision.kind,'enter');await rejected;assert.equal(f.engine.operations.size,0);
});
test('a flushed rollback receipt survives interrupted sidecar publication', async t => {
  const f=await fixture(t); await user(f,'before',1); finish(f,'done',1);
  const st=await f.engine.store.load(f.session.id);
  st.journal={id:'rollback-once',nativeStart:f.session.seq,root:f.cwd,changes:[],context:{atSeq:f.session.seq},metadata:{cwd:f.cwd,plan:false}};
  await f.engine.store.save(st);
  const save=f.engine.store.save.bind(f.engine.store);
  f.engine.store.save=async value=>{if(!value.journal)throw Error('sidecar unavailable');return save(value);};
  await assert.rejects(f.engine.recover(f.session,await f.engine.store.load(f.session.id)),/sidecar unavailable/);
  f.engine.store.save=save;
  const history=structuredClone(f.session.snapshotEvents());
  const cold=Session.create(f.session.id,history,f.session.header);
  const beforeSecondRecovery=structuredClone(cold.snapshotEvents());
  await f.engine.recover(cold,await f.engine.store.load(cold.id));
  assert.deepEqual(cold.snapshotEvents(),beforeSecondRecovery);assert.equal((await f.engine.store.load(cold.id)).journal,null);
});
test('idle expiry releases preview objects without another preview request', async t => {
  const f=await fixture(t), seq=await user(f,'preview',1);finish(f,'done',1);
  t.mock.timers.enable({apis:['setTimeout','Date'],now:Date.now()});
  const p=await f.engine.preview(f.session.id,seq);
  assert.ok(f.engine.plans.has(p.planId));
  t.mock.timers.tick(300001);
  assert.equal(f.engine.plans.size,0);await assert.rejects(f.engine.execute(f.session.id,p.planId),/过期/);
});

test('negative native durability receipt preserves recovery journal', async t => {
  const f = await fixture(t);
  f.ctx.sessions.flush = async () => false;
  const st = {journal:{id:'probe',after:{journal:null}}};
  const session = {snapshotEvents:()=>[{type:'plugin:dsh-rewind-plus/commit',data:{id:'probe'}}]};
  await assert.rejects(f.engine.recover(session,st,true,true), /持久化未确认/);
  assert.equal(st.journal.id,'probe');
});

test('failed maintenance read returns claimed user input to the inbox', async t => {
  const f = await fixture(t), user = {id:'input',source:{kind:'user'}}, marker = {id:'marker',source:{kind:'dsh-rewind-plus'}};
  f.engine.operations.set(f.agent.id,{agent:f.agent,marker:marker.id,queue:new Map([[user.id,'next-turn']])});
  f.engine.store.load = async () => {throw Error('storage unavailable');};
  const decision = await f.engine.preStep({agent:f.agent,messages:[marker,user],turn:1},()=>assert.fail());
  assert.equal(decision.kind,'reject'); assert.deepEqual(f.agent.inbox.nextTurn,[user]);
});

test('legacy disposable browser records never block rewind, undo or recovery',async t=>{
 const f=await fixture(t),seq=await user(f,'render',1),file=join(f.cwd,'page.html');
 await f.engine.tool({agent:f.agent},()=>writeFile(file,'page'));finish(f,'done',1);
 const st=await f.engine.store.load(f.session.id),path='.local/browser-checks/opencu-check-old/Default/Cache/entry';
 const entry={kind:'file',hash:'a'.repeat(64),mode:0o600};
 st.changes[0].changes.push({path,before:null,after:entry});
 st.changes.push({root:f.cwd,owners:[f.session.id],changes:[{path,before:{...entry,hash:'b'.repeat(64)},after:null}]});
 await f.engine.store.save(st);
 const plan=await f.engine.preview(f.session.id,seq);assert.equal(plan.canApply,true);assert.deepEqual(plan.files.map(f=>f.path),['page.html']);
 await f.engine.execute(f.session.id,plan.planId);await assert.rejects(readFile(file),{code:'ENOENT'});
 const undo=await f.engine.preview(f.session.id,null,true);await f.engine.execute(f.session.id,undo.planId);assert.equal(await readFile(file,'utf8'),'page');
 const recovered=await f.engine.files.recover(f.cwd,[{path,before:null,after:entry}],'after',{bestEffort:true});assert.deepEqual(recovered.skipped,[]);
});

test('confirm stops only the running conversation and its children automatically',async t=>{
 const f=await fixture(t),seq=await user(f,'running',1);finish(f,'done',1);
 const child={id:'child',status:'running',session:Session.create('child',undefined,{...Session.create('child').header,cwd:f.cwd,origin:'subagent',parentSession:f.agent.id})};
 const other={id:'foreign',status:'running',session:Session.create('foreign',undefined,{...Session.create('foreign').header,cwd:join(f.dir,'elsewhere')})};
 const agents=[f.agent,child,other],stopped=[];f.agent.status='running';
 for(const a of agents){a.cancel=()=>{stopped.push(a.id);a.status='idle';};a.whenIdle=async()=>assert.equal(a.status,'idle');}
 f.ctx.agents={list:()=>agents,get:id=>agents.find(a=>a.id===id)};
 const plan=await f.engine.preview(f.session.id,seq);assert.equal(plan.canApply,true);assert.equal(plan.canStop,true);
 await f.engine.execute(f.session.id,plan.planId);assert.deepEqual(stopped,[f.agent.id,child.id]);assert.equal(other.status,'running');assert.equal(messages(f.session).length,0);
});

test('missing backup bytes and an occupied workspace still allow conversation rewind',async t=>{
 for(const fault of ['missing-blob','workspace-lock']){
  const f=await fixture(t),file=join(f.cwd,'a');await writeFile(file,'A');const seq=await user(f,fault,1);
  await f.engine.tool({agent:f.agent},()=>writeFile(file,'B'));finish(f,'done',1);
  let release;
  if(fault==='missing-blob')f.engine.files.readBlob=async()=>{throw Error('backup missing');};
  else {const {lease,hash}=await import('../src/store.mjs');release=await lease(join(f.dir,'store/workspace-locks',hash(f.cwd)+'.json'));}
  try{const plan=await f.engine.preview(f.session.id,seq);await f.engine.execute(f.session.id,plan.planId);assert.equal(messages(f.session).length,0);assert.equal(await readFile(file,'utf8'),'B');assert.equal((await f.engine.store.load(f.session.id)).undo.at(-1).changes.length,0);}
  finally{await release?.();}
 }
});

test('interrupted recovery keeps externally edited files and lets confirmation finish',async t=>{
 const f=await fixture(t),file=join(f.cwd,'a');await writeFile(file,'A');const seq=await user(f,'edit',1);
 await f.engine.tool({agent:f.agent},()=>writeFile(file,'B'));finish(f,'done',1);
 const st=await f.engine.store.load(f.session.id),record=st.changes[0];
 st.journal={id:'interrupted',nativeStart:f.session.seq,root:f.cwd,changes:record.changes,context:{atSeq:f.session.seq},metadata:f.engine.metadata(f.session)};await f.engine.store.save(st);
 await writeFile(file,'manual');
 const plan=await f.engine.preview(f.session.id,seq);assert.equal(plan.canApply,true);
 await f.engine.execute(f.session.id,plan.planId);assert.equal(messages(f.session).length,0);assert.equal(await readFile(file,'utf8'),'manual');assert.equal((await f.engine.store.load(f.session.id)).journal,null);validateHistory(f.session);
});

test('an expired confirmation refreshes the same selected message without another dialog',async t=>{
 const f=await fixture(t),seq=await user(f,'edit',1);finish(f,'done',1);
 const plan=await f.engine.preview(f.session.id,seq);f.engine.plans.delete(plan.planId);
 await f.engine.execute(f.session.id,plan.planId,undefined,{seq,undo:false});assert.equal(messages(f.session).length,0);
});

test('undo records actual writes, not an external edit that happens to match the restore target',async t=>{
 for(const fault of ['external-match','after-write']){
  const f=await fixture(t),file=join(f.cwd,'a');await writeFile(file,'A');const seq=await user(f,fault,1);
  await f.engine.tool({agent:f.agent},()=>writeFile(file,'B'));finish(f,'done',1);
  if(fault==='external-match'){const read=f.engine.files.readBlob.bind(f.engine.files);f.engine.files.readBlob=async key=>{const bytes=await read(key);await writeFile(file,'A');return bytes;};}
  else {const put=f.engine.files.put.bind(f.engine.files);f.engine.files.put=async(...args)=>{await put(...args);throw Error('directory sync unavailable');};}
  const plan=await f.engine.preview(f.session.id,seq);await f.engine.execute(f.session.id,plan.planId);
  assert.equal(messages(f.session).length,0);assert.equal(await readFile(file,'utf8'),'A');
  const state=await f.engine.store.load(f.session.id);assert.equal(state.undo.at(-1).changes.length,fault==='external-match'?0:1);
 }
});
