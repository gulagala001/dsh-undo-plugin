import test from 'node:test';
import assert from 'node:assert/strict';
import { createConfirmation } from '../src/client/confirmation.mjs';

test('only the selected preview can admit one commit, even before the UI rerenders', () => {
  const review=createConfirmation(), a=review.preview({seq:1}), b=review.preview({seq:2});
  review.reviewed(b,{canApply:true,planId:'b'});review.fail(a,'late');review.reviewed(a,{planId:'a'});
  const commit=review.apply();assert.equal(commit.plan.planId,'b');assert.equal(commit.target.seq,2);
  assert.equal(review.preview({undo:true}),undefined);assert.equal(review.apply(),undefined);
  review.close();assert.equal(review.read(),commit);review.reset();review.fail(commit,'late');
  assert.equal(review.read().phase,'closed');
});
