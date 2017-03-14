#!/usr/bin/python
# -*- coding: <encoding name> -*-

import logging
from google.appengine.ext import ndb
from .task import Task

def run_test(task_id):
  '''For deferred.defer only. Get the task and call the run. simple as that.'''
  task_key = ndb.Key(urlsafe=task_id)
  task = task_key.get()
  task.run()

class FermatPrimalityTestWorkerTask(Task):
  '''The worker task to check whether the given number is a prime number with
  Fermat Little Theorem.'''

  def fast_mod(self, base, prime):
    '''not going to implement my version of modular exponentiation as Python
    already has one.

    If you are intereested, you can visit :

    Fast modular exponentiation (article) | Khan Academy
    https://www.khanacademy.org/computing/computer-science/cryptography/modarithmetic/a/fast-modular-exponentiation
    '''
    return pow(base, prime - 1, prime)

  def run(self):
    '''Compute the Fermat Primality Test for the given `a` and `p`.
    Basically, if `a^(p-1) % p = 1` is true, then we assume it is a prime
    number. But we might be fooled by this (a, p) pair.'''
    if self.results is None:
      # do work
      base, prime = self.inputs['base'], self.inputs['prime']
      mod = self.fast_mod(base, prime)
      self.results = {
        'is_prime': mod == 1,
        'mod': mod,
      }
      self.put()
    else:
      logging.debug('we have done the job somehow... should call the callback now.')

    self.callback()



class FermatPrimalityTestTask(Task):
  '''Run Fermat Primality Test for a given number.

  Attributes:
      rate (float): A floating point value to control the sampling rate. E.g.
          how many test cases we have to do.
  '''
  rate = 0.6

  def run(self):
    '''Create a bunch of subtasks to test the prime... It is all maths! For
    interested reader:

    - https://en.wikipedia.org/wiki/Fermat_primality_test
    - https://www.khanacademy.org/computing/computer-science/cryptography/random-algorithms-probability/v/fermat-primality-test-prime-adventure-part-10'''
    import random
    prime = self.inputs.get('prime')

    # according to maths, we don't really need to test all the numbers `a` in (1, prime)
    tests = range(2, prime)
    import math
    sample_size = int(math.ceil(len(tests) * self.rate))
    tests = random.sample(tests, sample_size)
    logging.debug(tests)

    from webapp2 import get_request
    req = get_request()
    callback_url = 'http://{host}/handle_fermat_worker'.format(host=req.host)

    # create the tasks.
    tasks = [FermatPrimalityTestWorkerTask(
      inputs={'prime': prime, 'base': base},
      parent_task=self.key,
      callback_url=callback_url) for base in tests]
    ndb.put_multi(tasks)

    # put into task queue to run the tests.
    from google.appengine.ext import deferred
    for task in tasks:
      deferred.defer(run_test, task_id=task.key.urlsafe())
