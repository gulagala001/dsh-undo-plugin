import test from 'node:test';
import assert from 'node:assert/strict';
import {installRewindShortcut} from '../src/client/entry-points.mjs';

test('double Escape respects native consumption, composition, focus, running state, expiry and disposal',async()=>{
 let listener,opened=0,consumed=0,target={id:'a',region:{},binding:{},open:()=>opened++};
 const dispose=installRewindShortcut({stopSequenceMs:20,observeFixedInput:fn=>{listener=fn;return()=>{listener=null;};}},{resolve:()=>target});
 const press=(gesture={},context={})=>listener({type:'keydown',gesture:{code:'Escape',repeat:false,composing:false,defaultPrevented:false,...gesture},context:{modal:null,region:'editable',...context},consume:()=>consumed++});
 try{
  press();assert.equal(opened,0);press();assert.equal(opened,1);assert.equal(consumed,1);
  for(const [g,c]of [[{repeat:true},{}],[{composing:true},{}],[{defaultPrevented:true},{}],[{shift:true},{}],[{code:'KeyA'},{}],[{},{modal:'confirm'}],[{},{region:'terminal'}]]){
   listener({type:'reset'});press();press(g,c);press();assert.equal(opened,1);
  }
  listener({type:'reset'});press();target={...target,binding:{}};press();assert.equal(opened,1);
  listener({type:'reset'});press();const saved=target;target=undefined;press();target=saved;press();assert.equal(opened,1);
  listener({type:'reset'});press();await new Promise(r=>setTimeout(r,30));press();assert.equal(opened,1);
  press();assert.equal(opened,2);
 }finally{dispose();}
 assert.equal(listener,null);
});
