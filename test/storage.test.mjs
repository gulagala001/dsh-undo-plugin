import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtemp,mkdir,writeFile,readFile,rm,open} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {randomBytes} from 'node:crypto';
import {Files,difference} from '../src/files.mjs';
import {atomic,Store} from '../src/store.mjs';
import {collectGarbage} from '../src/garbage.mjs';
import {Maintenance} from '../src/maintenance.mjs';

test('packed backup collection retains durable undo content and respects active owners',async t=>{
  const dir=await mkdtemp(join(tmpdir(),'rewind-storage-'));t.after(()=>rm(dir,{recursive:true,force:true}));
  const root=join(dir,'store'),workspace=join(dir,'workspace');await mkdir(workspace);
  const files=new Files(root),kept=Buffer.from('keep original undo bytes'),unused=randomBytes(2*1024*1024);
  await writeFile(join(workspace,'keep'),kept);await writeFile(join(workspace,'unused'),unused);
  const first=await files.capture(workspace);
  const warm=files.stats.bytesRead;await files.capture(workspace);assert.equal(files.stats.bytesRead,warm);
  await atomic(join(root,'sessions','fixture.json'),{schema:1,undo:[{hash:first.manifest.keep.hash}]});
  const owner=join(root,'owners','fixture.json');await atomic(owner,{pid:process.pid});
  assert.equal((await collectGarbage(root,{apply:true})).busy,true);await rm(owner);
  const report=await collectGarbage(root,{apply:true});assert.ok(report.reclaimedBytes>=unused.length);
  const cold=new Files(root);assert.deepEqual(await cold.readBlob(first.manifest.keep.hash),kept);
  await assert.rejects(cold.readBlob(first.manifest.unused.hash));
});

test('independent workspaces serialize shared pack writes without losing either backup',async t=>{
  const dir=await mkdtemp(join(tmpdir(),'rewind-packs-'));t.after(()=>rm(dir,{recursive:true,force:true}));
  const root=join(dir,'store'),a=join(dir,'a'),b=join(dir,'b');
  await Promise.all([mkdir(a),mkdir(b)]);await Promise.all([writeFile(join(a,'file'),'first'),writeFile(join(b,'file'),'second')]);
  const [one,two]=await Promise.all([new Files(root).capture(a),new Files(root).capture(b)]);
  const cold=new Files(root);
  assert.equal((await cold.readBlob(one.manifest.file.hash)).toString(),'first');
  assert.equal((await cold.readBlob(two.manifest.file.hash)).toString(),'second');
});

test('large files use bounded chunks, reuse unchanged chunks and ignore a tiny storage target',async t=>{
  const dir=await mkdtemp(join(tmpdir(),'rewind-chunks-'));t.after(()=>rm(dir,{recursive:true,force:true}));
  const root=join(dir,'store'),workspace=join(dir,'workspace');await mkdir(workspace);
  const original=randomBytes(9*1024*1024),file=join(workspace,'large.bin');
  await writeFile(file,original);const files=new Files(root,{storageTargetBytes:1});
  const before=await files.capture(workspace),stored=files.stats.storedBytes;
  const h=await open(file,'r+');try{await h.write(Buffer.from([original[4*1024*1024+7]^255]),0,1,4*1024*1024+7);}finally{await h.close();}
  const after=await files.capture(workspace);
  assert.equal(before.manifest['large.bin'].chunks.length,3);
  assert.ok(files.stats.storedBytes-stored<=4*1024*1024+1);
  await files.apply(before.root,difference(before.manifest,after.manifest));
  assert.deepEqual(await readFile(file),original);
});

test('retention retires old file history but preserves conversation masks, the newest undo and pending transactions',async t=>{
  const dir=await mkdtemp(join(tmpdir(),'rewind-retention-'));t.after(()=>rm(dir,{recursive:true,force:true}));
  const root=join(dir,'store'),workspace=join(dir,'workspace');await mkdir(workspace);
  await writeFile(join(workspace,'a'),'old');const files=new Files(root),old=(await files.capture(workspace)).manifest.a;
  await writeFile(join(workspace,'a'),'current');const current=(await files.capture(workspace)).manifest.a;
  const shape=(id,updatedAt,entry)=>({schema:1,id,revision:0,updatedAt,hiddenRanges:[{start:2,end:4}],internalRanges:[],checkpoints:{message:{changeIndex:0}},changes:[{changes:[{before:entry,after:entry}]}],undo:[{changes:[{before:entry,after:entry}]}]});
  const expired=shape('expired',Date.now()-31*86400000,old),latest=shape('current',Date.now(),current);
  await atomic(join(root,'sessions','old.json'),expired);await atomic(join(root,'sessions','new.json'),latest);
  const report=await collectGarbage(root,{apply:true,targetBytes:1});
  assert.equal(report.expiredSessions,1);assert.equal(report.aboveTarget,true);
  const oldState=JSON.parse(await readFile(join(root,'sessions','old.json'))),newState=JSON.parse(await readFile(join(root,'sessions','new.json')));
  assert.deepEqual(oldState.hiddenRanges,expired.hiddenRanges);assert.equal(oldState.undo.length,0);assert.equal(newState.undo.length,1);
  assert.equal((await new Files(root).readBlob(current.hash)).toString(),'current');
  const pending=shape('pending',Date.now()-31*86400000,current);pending.journal={changes:[{before:current,after:current}]};
  await atomic(join(root,'sessions','pending.json'),pending);await collectGarbage(root,{apply:true,targetBytes:0});
  assert.ok(JSON.parse(await readFile(join(root,'sessions','pending.json'))).journal);
});

test('runtime collection waits for unpublished captures and invalidates cached hashes afterwards',async t=>{
  const dir=await mkdtemp(join(tmpdir(),'rewind-idle-'));t.after(()=>rm(dir,{recursive:true,force:true}));
  const root=join(dir,'store'),workspace=join(dir,'workspace');await mkdir(workspace);
  await writeFile(join(workspace,'keep'),'saved');await writeFile(join(workspace,'unused'),randomBytes(2*1024*1024));
  const engine={store:new Store(root),files:new Files(root),operations:new Map(),ctx:{}},owner=join(root,'owners','fixture.json');
  await atomic(owner,{pid:process.pid,protocol:2,busy:false});
  const maintenance=new Maintenance(engine,owner);t.after(()=>maintenance.dispose());
  let manifest;
  await maintenance.run(async()=>{
    manifest=(await engine.files.capture(workspace)).manifest;
    assert.equal((await collectGarbage(root,{apply:true})).busy,true);
    await engine.store.save({schema:1,id:'active',revision:0,checkpoints:{m:{changeIndex:0}},changes:[{before:manifest.keep}],undo:[],hiddenRanges:[]});
  });
  const result=await maintenance.collect();assert.ok(result.reclaimedBytes>=2*1024*1024);
  await maintenance.run(async()=>{
    await engine.files.capture(workspace);
    assert.equal((await engine.files.readBlob(manifest.keep.hash)).toString(),'saved');
    assert.equal((await engine.files.readBlob(manifest.unused.hash)).length,2*1024*1024);
  });
});

test('all legal file names survive capture, JSON persistence, creation and deletion',async t=>{
  const dir=await mkdtemp(join(tmpdir(),'rewind-names-'));t.after(()=>rm(dir,{recursive:true,force:true}));
  const root=join(dir,'store'),workspace=join(dir,'workspace');await mkdir(workspace);
  const names=['__proto__','constructor','toString','hasOwnProperty'];
  const files=new Files(root),empty=(await files.capture(workspace)).manifest;
  for(const name of names)await writeFile(join(workspace,name),'original '+name);
  const captured=await files.capture(workspace),saved=JSON.parse(JSON.stringify(captured.manifest));
  assert.deepEqual(Object.keys(saved).sort(),names.sort());
  assert.ok(difference(empty,saved).every(c=>c.before===null));
  assert.ok(difference(saved,empty).every(c=>c.after===null));
  await files.apply(captured.root,difference(empty,saved));
  assert.deepEqual((await files.capture(workspace)).manifest,{});
  await files.apply(captured.root,difference(saved,empty));
  for(const name of names)assert.equal(await readFile(join(workspace,name),'utf8'),'original '+name);
});

test('restore does not overwrite an edit made while backup bytes are being read',async t=>{
 for(const missing of [false,true])await t.test(missing?'deleted destination':'existing destination',async t=>{
  const dir=await mkdtemp(join(tmpdir(),'rewind-write-race-'));t.after(()=>rm(dir,{recursive:true,force:true}));
  const root=join(dir,'store'),workspace=join(dir,'workspace');await mkdir(workspace);
  const path=join(workspace,'file'),files=new Files(root);
  await writeFile(path,'original');const before=await files.capture(workspace);
  if(missing)await rm(path);else await writeFile(path,'tool edit');const after=await files.capture(workspace);
  const readBlob=files.readBlob.bind(files);
  files.readBlob=async key=>{const bytes=await readBlob(key);await writeFile(path,'external edit');return bytes;};
  await assert.rejects(files.apply(before.root,difference(before.manifest,after.manifest)),/文件.*改动|文件.*变化/);
  assert.equal(await readFile(path,'utf8'),'external edit');
 });
});

test('recovery rechecks each write after preflight, including deletions',async t=>{
  for(const created of [false,true])await t.test(created?'new file':'existing file',async t=>{
    const dir=await mkdtemp(join(tmpdir(),'rewind-recovery-race-'));t.after(()=>rm(dir,{recursive:true,force:true}));
    const root=join(dir,'store'),workspace=join(dir,'workspace');await mkdir(workspace);
    const files=new Files(root);
    await writeFile(join(workspace,'a'),'A');
    if(!created)await writeFile(join(workspace,'b'),'A');
    const before=await files.capture(workspace);
    await writeFile(join(workspace,'a'),'B');await writeFile(join(workspace,'b'),'B');
    const after=await files.capture(workspace),changes=difference(before.manifest,after.manifest);
    const put=files.put.bind(files);
    files.put=async(...args)=>{await put(...args);if(args[1]==='a')await writeFile(join(workspace,'b'),'external edit');};
    await assert.rejects(files.recover(before.root,changes,'before'),/文件.*改动|文件.*变化/);
    assert.equal(await readFile(join(workspace,'a'),'utf8'),'A');
    assert.equal(await readFile(join(workspace,'b'),'utf8'),'external edit');
  });
});
