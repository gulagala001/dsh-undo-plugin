import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import {syncBuiltinESMExports} from 'node:module';
import {join} from 'node:path';
import {tmpdir} from 'node:os';
import {randomBytes} from 'node:crypto';
import {Files} from '../src/files.mjs';
import {atomic,Store,hash} from '../src/store.mjs';
import {Maintenance} from '../src/maintenance.mjs';
import {collectGarbage} from '../src/garbage.mjs';

async function fixture(t){
  const dir=await fs.mkdtemp(join(tmpdir(),'rewind-disk-'));
  t.after(()=>fs.rm(dir,{recursive:true,force:true}));
  const root=join(dir,'store'),workspace=join(dir,'workspace');await fs.mkdir(workspace);
  return {dir,root,workspace};
}
function restoreMocks(t){t.after(()=>{t.mock.restoreAll();syncBuiltinESMExports();});syncBuiltinESMExports();}

test('a short capture is collected after becoming idle without another operation',async t=>{
  const {root,workspace}=await fixture(t);
  await fs.writeFile(join(workspace,'unchanged.bin'),randomBytes(2*1024*1024));
  const engine={store:new Store(root),files:new Files(root),operations:new Map(),ctx:{}};
  const maintenance=new Maintenance(engine,join(root,'owners','test.json'));
  t.after(()=>maintenance.dispose());
  t.mock.timers.enable({apis:['Date','setTimeout']});
  let collection;const collect=maintenance.collect.bind(maintenance);
  t.mock.method(maintenance,'collect',()=>collection=collect());
  await maintenance.run(()=>engine.files.capture(workspace));
  assert.ok((await fs.readdir(join(root,'packs'))).length);
  t.mock.timers.tick(31000);
  assert.ok(collection,'idle cleanup must be scheduled even below the time/growth threshold');
  await collection;
  assert.equal((await fs.readdir(join(root,'packs'))).length,0);
});

test('short recurring operations do not postpone an already due collection',async t=>{
  const {root}=await fixture(t);
  const engine={store:new Store(root),files:new Files(root),operations:new Map(),ctx:{}};
  const maintenance=new Maintenance(engine,join(root,'owners','test.json'));
  t.after(()=>maintenance.dispose());
  t.mock.timers.enable({apis:['Date','setTimeout']});
  maintenance.lastSweep=Date.now()-30000;
  let collection;const collect=maintenance.collect.bind(maintenance);
  t.mock.method(maintenance,'collect',()=>collection=collect());
  await maintenance.run(async()=>{});
  t.mock.timers.tick(500);await maintenance.run(async()=>{});
  t.mock.timers.tick(500);
  assert.ok(collection,'new operations must not restart the cleanup delay');
  await collection;
});

test('storage aliases cannot bypass the workspace overlap guard',async t=>{
  const {dir,workspace}=await fixture(t);
  await fs.writeFile(join(workspace,'source.txt'),'source');
  const alias=join(dir,'alias');await fs.symlink(workspace,alias,process.platform==='win32'?'junction':'dir');
  const files=new Files(join(alias,'backup'));
  await assert.rejects(files.capture(workspace),/备份目录/);
});

test('low free space stops backup writes before consuming the remaining disk',async t=>{
  const {root,workspace}=await fixture(t);
  await fs.writeFile(join(workspace,'file.bin'),randomBytes(2*1024*1024));
  t.mock.method(fs,'statfs',async()=>({bavail:16,bsize:4096}));restoreMocks(t);
  await assert.rejects(new Files(root).capture(workspace),{code:'REWIND_DISK_SPACE'});
  assert.deepEqual(await fs.readdir(join(root,'packs')),[]);
});

test('failed atomic metadata writes remove their temporary file',async t=>{
  const {root}=await fixture(t);const originalOpen=fs.open;
  t.mock.method(fs,'open',async(...args)=>{
    const handle=await originalOpen(...args);
    if(String(args[0]).endsWith('.tmp'))handle.writeFile=async()=>{throw Object.assign(Error('disk full'),{code:'ENOSPC'});};
    return handle;
  });restoreMocks(t);
  await assert.rejects(atomic(join(root,'state.json'),{saved:true}),{code:'ENOSPC'});
  assert.deepEqual(await fs.readdir(root),[]);
});

test('a pack write that runs out of space removes its unpublished partial pack',async t=>{
  const {root,workspace}=await fixture(t);
  await fs.writeFile(join(workspace,'file.bin'),randomBytes(2*1024*1024));
  const originalOpen=fs.open;
  t.mock.method(fs,'open',async(...args)=>{
    const handle=await originalOpen(...args);
    if(String(args[0]).endsWith('.pack')&&args[1]==='wx'){
      const write=handle.writeFile.bind(handle);
      handle.writeFile=async bytes=>{await write(bytes.subarray(0,32));throw Object.assign(Error('disk full'),{code:'ENOSPC'});};
    }
    return handle;
  });restoreMocks(t);
  await assert.rejects(new Files(root).capture(workspace),{code:'ENOSPC'});
  assert.deepEqual(await fs.readdir(join(root,'packs')),[]);
  await assert.rejects(fs.stat(join(root,'blob-index.json')),{code:'ENOENT'});
});

test('collection frees orphan packs before deferring compaction on a nearly full disk',async t=>{
  const {root,workspace}=await fixture(t);
  await fs.writeFile(join(workspace,'keep'),'keep');
  await fs.writeFile(join(workspace,'unused'),randomBytes(2*1024*1024));
  const captured=await new Files(root).capture(workspace);
  await atomic(join(root,'sessions','test.json'),{schema:1,undo:[{hash:captured.manifest.keep.hash}]});
  const orphan=join(root,'packs','orphan.pack');await fs.writeFile(orphan,'orphan');
  t.mock.method(fs,'statfs',async()=>({bavail:16,bsize:4096}));restoreMocks(t);
  const result=await collectGarbage(root,{apply:true});
  assert.equal(result.rewriteDeferred,true);
  await assert.rejects(fs.stat(orphan),{code:'ENOENT'});
  assert.equal((await new Files(root).readBlob(captured.manifest.keep.hash)).toString(),'keep');
  assert.equal((await fs.readdir(join(root,'packs'))).length,1);
});

test('legacy loose backups count toward the retention target',async t=>{
  const {root}=await fixture(t);await fs.mkdir(join(root,'blobs'),{recursive:true});
  const bytes=randomBytes(2*1024*1024),key=hash(bytes);
  await fs.writeFile(join(root,'blobs',key),Buffer.concat([Buffer.from([0]),bytes]));
  const state=(id,time,changes)=>({schema:1,id,revision:0,updatedAt:time,hiddenRanges:[],checkpoints:{m:{changeIndex:0,createdAt:time}},changes,undo:[]});
  await atomic(join(root,'sessions','old.json'),state('old',Date.now()-1000,[{before:{hash:key}}]));
  await atomic(join(root,'sessions','new.json'),state('new',Date.now(),[]));
  const result=await collectGarbage(root,{apply:true,targetBytes:1024*1024});
  assert.equal(result.retiredCheckpoints,1);
  assert.equal(result.reclaimedBytes,bytes.length+1);
  await assert.rejects(fs.stat(join(root,'blobs',key)),{code:'ENOENT'});
});

test('idle collection retries when another owner finishes an unpublished capture',async t=>{
  const {root,workspace}=await fixture(t);
  await fs.writeFile(join(workspace,'unused'),randomBytes(2*1024*1024));
  const engine={store:new Store(root),files:new Files(root),operations:new Map(),ctx:{}};
  const maintenance=new Maintenance(engine,join(root,'owners','test.json'));
  t.after(()=>maintenance.dispose());
  t.mock.timers.enable({apis:['Date','setTimeout']});
  let collection;const collect=maintenance.collect.bind(maintenance);
  t.mock.method(maintenance,'collect',()=>collection=collect());
  await maintenance.run(()=>engine.files.capture(workspace));
  const other=join(root,'owners','other.json');await atomic(other,{pid:process.pid,protocol:2,busy:true});
  t.mock.timers.tick(31000);
  const first=collection;assert.equal((await first).busy,true);
  assert.ok((await fs.readdir(join(root,'packs'))).length);
  await atomic(other,{pid:process.pid,protocol:2,busy:false});
  t.mock.timers.tick(1000);assert.notEqual(collection,first);
  await collection;assert.equal((await fs.readdir(join(root,'packs'))).length,0);
});

test('a large capture hits its write ceiling and leaves existing recovery bytes readable',async t=>{
  const {root,workspace}=await fixture(t),files=new Files(root,{maxStorageBytes:10*1024*1024});
  await fs.writeFile(join(workspace,'keep'),'original');
  const before=await files.capture(workspace);
  await atomic(join(root,'sessions','test.json'),{schema:1,undo:[{hash:before.manifest.keep.hash}]});
  await fs.writeFile(join(workspace,'large.bin'),randomBytes(16*1024*1024));
  await assert.rejects(files.capture(workspace),{code:'REWIND_STORAGE_LIMIT'});
  let bytes=0;for(const name of await fs.readdir(join(root,'packs')))bytes+=(await fs.stat(join(root,'packs',name))).size;
  assert.ok(bytes<10*1024*1024);
  assert.equal((await new Files(root).readBlob(before.manifest.keep.hash)).toString(),'original');
  await collectGarbage(root,{apply:true});files.invalidateStorage();
  await fs.rm(join(workspace,'large.bin'));await fs.writeFile(join(workspace,'keep'),'continued');
  assert.equal((await files.capture(workspace)).manifest.keep.hash,hash('continued'));
});

test('independent writers share a physical backup ceiling under the pack lock',async t=>{
  const {root,workspace,dir}=await fixture(t),other=join(dir,'other');await fs.mkdir(other);
  await fs.writeFile(join(workspace,'file'),randomBytes(2*1024*1024));
  await fs.writeFile(join(other,'file'),randomBytes(2*1024*1024));
  const results=await Promise.allSettled([workspace,other].map(path=>new Files(root,{maxStorageBytes:3*1024*1024}).capture(path)));
  assert.equal(results.filter(result=>result.status==='fulfilled').length,1);
  assert.equal(results.find(result=>result.status==='rejected').reason.code,'REWIND_STORAGE_LIMIT');
  const manifest=results.find(result=>result.status==='fulfilled').value.manifest;
  assert.equal((await new Files(root).readBlob(manifest.file.hash)).length,2*1024*1024);
  let bytes=0;for(const name of await fs.readdir(join(root,'packs')))bytes+=(await fs.stat(join(root,'packs',name))).size;
  assert.ok(bytes<3*1024*1024);
});

test('large metadata cannot consume disk headroom while small cleanup ownership stays writable',async t=>{
  const {root}=await fixture(t);
  let available=16;
  t.mock.method(fs,'statfs',async()=>({bavail:available,bsize:4096}));restoreMocks(t);
  await assert.rejects(atomic(join(root,'large.json'),{data:'x'.repeat(2*1024*1024)}),{code:'REWIND_DISK_SPACE'});
  await atomic(join(root,'owner.json'),{pid:process.pid,protocol:2,busy:false});
  assert.deepEqual(await fs.readdir(root),['owner.json']);
  available=1024;
  await atomic(join(root,'cleanup.json'),{data:'x'.repeat(2*1024*1024)},{reserveBytes:0});
  assert.ok((await fs.stat(join(root,'cleanup.json'))).size>2*1024*1024);
});

test('an ENOSPC idle-marker failure cannot strand a busy owner and block cleanup',async t=>{
  const {root,workspace}=await fixture(t);await fs.writeFile(join(workspace,'unused'),'unused');
  const engine={store:new Store(root),files:new Files(root),operations:new Map(),ctx:{}};
  const owner=join(root,'owners','test.json'),maintenance=new Maintenance(engine,owner);
  t.after(()=>maintenance.dispose());
  const originalOpen=fs.open;
  t.mock.method(fs,'open',async(...args)=>{
    const handle=await originalOpen(...args);
    if(String(args[0]).startsWith(owner+'.')){
      const write=handle.writeFile.bind(handle);
      handle.writeFile=async content=>{if(content.includes('"busy":false'))throw Object.assign(Error('disk full'),{code:'ENOSPC'});return write(content);};
    }
    return handle;
  });restoreMocks(t);
  await maintenance.run(()=>engine.files.capture(workspace));
  await assert.rejects(fs.stat(owner),{code:'ENOENT'});
  assert.equal((await maintenance.collect()).busy,false);
  assert.deepEqual(await fs.readdir(join(root,'packs')),[]);
});

test('a directory-sync failure after index publication preserves the referenced pack',async t=>{
  const {root,workspace}=await fixture(t),bytes=Buffer.from('published backup');
  await fs.writeFile(join(workspace,'file'),bytes);
  const originalOpen=fs.open;let fail=true;
  t.mock.method(fs,'open',async(...args)=>{
    const handle=await originalOpen(...args);
    if(String(args[0])===root&&args[1]==='r'){
      const sync=handle.sync.bind(handle);
      handle.sync=async()=>{if(fail){fail=false;throw Object.assign(Error('sync failed'),{code:'EIO'});}return sync();};
    }
    return handle;
  });restoreMocks(t);
  await assert.rejects(new Files(root).capture(workspace),{code:'EIO'});
  assert.deepEqual(await new Files(root).readBlob(hash(bytes)),bytes);
  assert.equal((await fs.readdir(join(root,'packs'))).length,1);
});

test('failed compaction removes its temporary pack without discarding live recovery data',async t=>{
  const {root,workspace}=await fixture(t);
  await fs.writeFile(join(workspace,'keep'),'keep');await fs.writeFile(join(workspace,'unused'),randomBytes(2*1024*1024));
  const before=await new Files(root).capture(workspace);
  await atomic(join(root,'sessions','test.json'),{schema:1,undo:[{hash:before.manifest.keep.hash}]});
  const originalOpen=fs.open;let fail=true;
  t.mock.method(fs,'open',async(...args)=>{
    const handle=await originalOpen(...args);
    if(String(args[0]).endsWith('.pack')&&args[1]==='r'){
      const read=handle.read.bind(handle);
      handle.read=async(...args)=>{if(fail){fail=false;throw Object.assign(Error('read failed'),{code:'EIO'});}return read(...args);};
    }
    return handle;
  });restoreMocks(t);
  await assert.rejects(collectGarbage(root,{apply:true}),{code:'EIO'});
  assert.equal((await fs.readdir(join(root,'packs'))).length,1);
  assert.equal((await new Files(root).readBlob(before.manifest.keep.hash)).toString(),'keep');
});
