import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtemp,mkdir,writeFile,readFile,rm,open} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {Store,lease} from '../src/store.mjs';
import {RewindEngine} from '../src/engine.mjs';
import {Maintenance} from '../src/maintenance.mjs';

async function fixture(t){
  const root=await mkdtemp(join(tmpdir(),'rewind-lock-'));
  t.after(()=>rm(root,{recursive:true,force:true}));
  return {root,store:new Store(root),file:join(root,'lock.json')};
}
async function deadline(promise){
  let timer;try{return await Promise.race([promise,new Promise(resolve=>{timer=setTimeout(()=>resolve('hung'),250);})]);}
  finally{clearTimeout(timer);}
}

for(const mode of ['cancel','timeout'])test(`${mode} releases only the queued waiter and never starts its callback later`,async t=>{
  const {store}=await fixture(t),gate=Promise.withResolvers(),entered=Promise.withResolvers(),controller=new AbortController();
  let late=0,overlap=false,running=true;
  const owner=store.lock('workspace',async()=>{entered.resolve();await gate.promise;running=false;});
  t.after(()=>gate.resolve());await entered.promise;
  const waiting=store.lock('workspace',()=>late++,{signal:controller.signal,waitMs:mode==='timeout'?20:undefined});
  const observed=waiting.then(()=>null,error=>error);
  if(mode==='cancel')controller.abort(Error('cancel queued backup'));
  const result=await deadline(observed);
  assert.notEqual(result,'hung');assert.ok(result instanceof Error);
  if(mode==='timeout')assert.equal(result.code,'REWIND_LOCK_TIMEOUT');
  const later=store.lock('workspace',()=>{overlap=running;});
  await new Promise(resolve=>setImmediate(resolve));assert.equal(running,true);assert.equal(late,0);
  gate.resolve();await Promise.all([owner,waiting.catch(()=>{}),later]);
  assert.equal(late,0);assert.equal(overlap,false);assert.equal(store.tails.size,0);
});

test('cancelling an admitted owner does not release its active operation',async t=>{
  const {store}=await fixture(t),gate=Promise.withResolvers(),entered=Promise.withResolvers(),controller=new AbortController();
  let second=false;
  const owner=store.lock('workspace',async()=>{entered.resolve();await gate.promise;},{signal:controller.signal,waitMs:20});
  t.after(()=>gate.resolve());await entered.promise;controller.abort();
  const later=store.lock('workspace',()=>{second=true;});
  await new Promise(resolve=>setTimeout(resolve,30));assert.equal(second,false);
  gate.resolve();await Promise.all([owner,later]);assert.equal(second,true);
});

test('live and malformed process locks time out without deleting the owner record',async t=>{
  const {file}=await fixture(t);
  for(const record of [JSON.stringify({pid:process.pid,nonce:'other-owner'}),'']){
    await writeFile(file,record);
    await assert.rejects(lease(file,{waitMs:20}),{code:'REWIND_LOCK_TIMEOUT'});
    assert.equal(await readFile(file,'utf8'),record);
  }
  await assert.rejects(lease(file,{waitMs:Infinity}),TypeError);
  await rm(file);const release=await lease(file);await release();
  await assert.rejects(readFile(file),{code:'ENOENT'});
});

test('failed or cancelled lock publication removes its own file and preserves a replacement',async t=>{
  const {root,file}=await fixture(t);
  const handle=await open(join(root,'prototype'),'w'),prototype=Object.getPrototypeOf(handle),write=prototype.writeFile;
  await handle.close();
  for(const mode of ['failure','cancel','replacement']){
    const controller=new AbortController();
    const mock=t.mock.method(prototype,'writeFile',async function(...args){
      if(mode==='failure')throw Error('publication failed');
      await write.apply(this,args);
      if(mode==='replacement'){
        await rm(file);const replacement=await open(file,'wx');
        try{await write.call(replacement,'replacement owner');}finally{await replacement.close();}
      }
      controller.abort(Error('cancel publication'));
    });
    try{
      await assert.rejects(lease(file,{signal:controller.signal}),/publication/);
      if(mode==='replacement')assert.equal(await readFile(file,'utf8'),'replacement owner');
      else await assert.rejects(readFile(file),{code:'ENOENT'});
    }finally{mock.mock.restore();}
  }
});

test('a queued tool can be cancelled while another capture is still running',async t=>{
  const {root}=await fixture(t),workspace=join(root,'workspace');await mkdir(workspace);
  const agent={id:'session',session:{id:'session',header:{cwd:workspace},append(){}}};
  const engine=new RewindEngine({agents:{get:()=>agent}},join(root,'store'));
  engine.root=async()=>workspace;
  const gate=Promise.withResolvers(),entered=Promise.withResolvers(),controller=new AbortController();let captures=0,calls=0;
  engine.files.capture=async()=>{if(++captures===1){entered.resolve();await gate.promise;}return {manifest:{}};};
  const owner=engine.tool({agent},()=>calls++);t.after(()=>gate.resolve());await entered.promise;
  const waiting=engine.tool({agent,signal:controller.signal},()=>{calls+=100;});
  const observed=waiting.then(()=>null,error=>error);await new Promise(resolve=>setImmediate(resolve));
  controller.abort(Error('cancel queued tool'));
  assert.match((await deadline(observed))?.message??'',/cancel queued tool/);
  gate.resolve();await owner;await observed;
  assert.equal(calls,1);assert.equal(engine.waves.size,0);
});

test('workspace restoration contention cannot fall through to an unprotected tool',async t=>{
  const {root}=await fixture(t),agent={id:'session',session:{id:'session',header:{cwd:root},append(){}}};
  const engine=new RewindEngine({agents:{get:()=>agent}},join(root,'store'));engine.root=async()=>root;
  engine.busy.add(root);let calls=0;
  await assert.rejects(engine.tool({agent},()=>calls++),{code:'REWIND_WORKSPACE_BUSY'});
  assert.equal(calls,0);assert.equal(engine.waves.size,0);
});

test('maintenance admission observes cancellation without entering the operation',async t=>{
  const {root,store}=await fixture(t),gate=Promise.withResolvers(),entered=Promise.withResolvers(),controller=new AbortController();
  const engine={store,files:{stats:{storedBytes:0},invalidateStorage(){}},operations:new Map(),ctx:{}};
  const maintenance=new Maintenance(engine,join(root,'owner.json'));
  const owner=store.lock('maintenance-owner',async()=>{entered.resolve();await gate.promise;});
  t.after(()=>gate.resolve());await entered.promise;let calls=0;
  const waiting=maintenance.run(()=>calls++,undefined,{signal:controller.signal});
  const observed=waiting.then(()=>null,error=>error);controller.abort(Error('cancel maintenance'));
  assert.match((await deadline(observed))?.message??'',/cancel maintenance/);
  gate.resolve();await owner;await observed;assert.equal(calls,0);assert.equal(maintenance.active,0);
});
