import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtemp,mkdir,writeFile,readFile,rm,symlink,readlink} from 'node:fs/promises';
import {join} from 'node:path';
import {tmpdir} from 'node:os';
import {Session} from '@deepseek-ai/dsh-session';
import {createUserMessage,createMessage} from '@deepseek-ai/dsh-llm';
import {RewindEngine} from '../src/engine.mjs';
import {snapshot,prepareReplay,appendPrepared,validateHistory} from '../src/conversation.mjs';
const messages=s=>s.deriveMessages().map(({id,...m})=>m);
async function fixture(t){const dir=await mkdtemp(join(tmpdir(),'rewind-'));t.after(()=>rm(dir,{recursive:true,force:true}));const cwd=join(dir,'project');await mkdir(cwd);const session=Session.create('same-session',undefined,{...Session.create('same-session').header,cwd});const agent={id:session.id,session,status:'idle',runMaintenance:fn=>fn()};const ctx={sessions:{get:()=>session,flush:async()=>{},messageProjections:[]},agents:{get:()=>agent,list:()=>[agent]}};const engine=new RewindEngine(ctx,join(dir,'store'));agent.inbox={nextTurn:[],nextStep:[],prepend(target,m){this[target==='next-turn'?'nextTurn':'nextStep'].unshift(m);}};agent.steer=m=>queueMicrotask(async()=>{const turn=(session.snapshotEvents().findLast(e=>e.type==='turn/start')?.data.turn||0)+1;session.append('turn/start',{turn});await engine.preStep({agent,messages:[m],turn,step:1},()=>{throw Error('maintenance requested model');});const end=session.append('turn/end',{turn,reason:{kind:'blocked'}});engine.finish(session,end);});return {dir,cwd,session,agent,engine,ctx};}
async function user(f,text,turn){const m=createUserMessage({source:{kind:'user'},content:[{type:'text',text}]});f.session.append('turn/start',{turn});await f.engine.checkpoint(f.agent,[m]);const e=f.session.append('user/message',m,{surfaceOp:'append'});f.session.append('step/start',{turn,step:1});return e.seq;}
function finish(f,text,turn){f.session.append('assistant/message',{turn,step:1,stream:[],message:createMessage({role:'assistant',source:{kind:'model',provider:'test',model:'test'},content:[{type:'text',text}]})},{surfaceOp:'append'});f.session.append('step/end',{turn,step:1});f.session.append('turn/end',{turn,reason:{kind:'completed'}});}
test('plain chat records a checkpoint without scanning files and backup failures do not prevent tools',async t=>{
 const f=await fixture(t);let captures=0;
 f.engine.files.capture=async()=>{captures++;throw Error('synthetic backup unavailable');};
 const seq=await user(f,'chat',1);assert.equal(captures,0);
 let ran=false;await f.engine.tool({agent:f.agent},async()=>{ran=true;});assert.equal(ran,true);
 finish(f,'done',1);const plan=await f.engine.preview(f.session.id,seq);assert.equal(plan.canApply,false);assert.match(plan.reason,/未完整备份/);
});
test('cold history can read rewind state without activating an agent, then preview resumes it',async t=>{
 const f=await fixture(t),seq=await user(f,'cold request',1);finish(f,'done',1);
 const st=await f.engine.store.load(f.session.id);st.hiddenRanges=[{start:seq,end:seq}];await f.engine.store.save(st);
 let active=false,resumes=0;
 f.ctx.sessions.get=()=>active?f.session:undefined;f.ctx.agents.get=()=>active?f.agent:undefined;
 f.ctx.sessionController={inspect:async()=>({events:f.session.snapshotEvents()}),resolveAgent:async()=>{resumes++;active=true;return {agent:f.agent};}};
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
 f.ctx.sessionController={inspect:async()=>({events:f.session.snapshotEvents()})};f.engine.files.capture=()=>{throw Error('picker must not scan files');};
 const first=await f.engine.candidates(f.session.id);assert.equal(first.items.length,100);assert.equal(first.items[0].seq,seqs[103]);assert.ok(first.items.every(i=>i.text.length<=240));assert.equal(first.before,seqs[4]);
 const second=await f.engine.candidates(f.session.id,first.before);assert.deepEqual(second.items.map(i=>i.seq),seqs.slice(0,4).reverse());assert.ok(second.items.every(i=>i.conversationOnly));assert.equal(second.before,null);
 await assert.rejects(f.engine.candidates(f.session.id,-1),/无效/);
});
test('manual edits and stale confirmation do not get overwritten',async t=>{const f=await fixture(t);await writeFile(join(f.cwd,'a'),'A');const seq=await user(f,'edit',1);await f.engine.tool({agent:f.agent},()=>writeFile(join(f.cwd,'a'),'B'));finish(f,'done',1);const p=await f.engine.preview(f.session.id,seq);await writeFile(join(f.cwd,'a'),'manual');await assert.rejects(f.engine.execute(f.session.id,p.planId),/新的改动/);assert.equal(await readFile(join(f.cwd,'a'),'utf8'),'manual');assert.equal(f.session.deriveMessages().length,2);});
test('restore failure compensates already written files and keeps undo evidence',async t=>{const f=await fixture(t);await writeFile(join(f.cwd,'a'),'A');await writeFile(join(f.cwd,'b'),'A');const seq=await user(f,'edit',1);await f.engine.tool({agent:f.agent},async()=>{await writeFile(join(f.cwd,'a'),'B');await writeFile(join(f.cwd,'b'),'B');});finish(f,'done',1);const p=await f.engine.preview(f.session.id,seq);const put=f.engine.files.put.bind(f.engine.files);let fail=true;f.engine.files.put=async(root,path,value)=>{if(path==='b'&&fail){fail=false;throw Error('injected write failure');}return put(root,path,value);};await assert.rejects(f.engine.execute(f.session.id,p.planId),/injected/);assert.equal(await readFile(join(f.cwd,'a'),'utf8'),'B');assert.equal(await readFile(join(f.cwd,'b'),'utf8'),'B');assert.equal(f.session.deriveMessages().length,2);assert.equal((await f.engine.store.load(f.session.id)).journal,null);});
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
 const plan=await f.engine.preview(f.session.id,seq);assert.equal(plan.canApply,false);assert.match(plan.reason,/跨工作目录/);
 assert.equal(await readFile(join(other,'a'),'utf8'),'outside');
});
test('overlapping sessions are recorded as ambiguous and never overwrite each other',async t=>{
 const f=await fixture(t);await writeFile(join(f.cwd,'a'),'A');const seq=await user(f,'parallel',1);
 const other={id:'other',session:Session.create('other',undefined,{...Session.create('other').header,cwd:f.cwd})};let release,started;const began=new Promise(r=>started=r),wait=new Promise(r=>release=r);
 const first=f.engine.tool({agent:f.agent},async()=>{started();await wait;await writeFile(join(f.cwd,'a'),'first');});await began;
 await f.engine.tool({agent:other},()=>writeFile(join(f.cwd,'b'),'other'));release();await first;finish(f,'done',1);
 const plan=await f.engine.preview(f.session.id,seq);assert.equal(plan.canApply,false);assert.match(plan.reason,/其他会话/);assert.equal(await readFile(join(f.cwd,'b'),'utf8'),'other');
});
test('interrupted file transaction is recovered from disk before another mutation',async t=>{
 const f=await fixture(t);await writeFile(join(f.cwd,'a'),'A');await user(f,'edit',1);await f.engine.tool({agent:f.agent},()=>writeFile(join(f.cwd,'a'),'B'));finish(f,'done',1);
 const st=await f.engine.store.load(f.session.id),context=snapshot(f.session),record=st.changes[0];st.journal={id:'interrupted',nativeStart:f.session.seq,root:record.root,changes:record.changes,context,metadata:f.engine.metadata(f.session)};await f.engine.store.save(st);
 await f.engine.files.apply(record.root,record.changes);
 const {Store}=await import('../src/store.mjs'),{Files}=await import('../src/files.mjs');f.engine.store=new Store(join(f.dir,'store'));f.engine.files=new Files(join(f.dir,'store'));
 await f.engine.launch(f.session.id,{recovery:true});assert.equal(await readFile(join(f.cwd,'a'),'utf8'),'B');assert.equal((await f.engine.store.load(f.session.id)).journal,null);validateHistory(f.session);
});
