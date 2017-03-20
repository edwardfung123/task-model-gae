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


@ndb.transactional(retries=10)
def update_parent_task_finished(parent_task_key, val):
  parent_task = parent_task_key.get()
  parent_task.num_finished += val
  parent_task.put()
  return True

def worker_callback(task_id):
  logging.debug('In worker_callback')
  logging.debug('task_id = ' + task_id)
  task_key = ndb.Key(urlsafe=task_id)
  task = task_key.get()
  if task is None:
    raise ValueError('The task does not exist. really?')
  logging.debug(task.inputs)
  logging.debug(task.results)
  # check if all other workers are done too.
  parent_task = task.parent_task.get()
  if parent_task is None:
    raise ValueError('The parent task does not exist.')

  # If we are lucky enough, the checking can be run in multiple times in
  # parallel. Perhaps it is important to know it. In this case, we don't
  # have any problem running this multiple times. just silly.
  update_parent_task_finished(task.parent_task, 1)

  parent_task = task.parent_task.get()

  if parent_task.num_finished == parent_task.num_subtasks:
    logging.debug('All the subtasks have finished.')
    is_prime = True
    # Try to loop through the sub-tasks.
    for task in parent_task.subtasks:
      if task.results['is_prime'] is False:
        logging.debug('Find a subtask that said the number is not a prime.')
        is_prime = False
        break
    parent_task.results = {'is_prime': is_prime}
    parent_task.put()
    parent_task.callback()
  else:
    # fine then. we wait for another callback.
    pass

class FermatPrimalityTestTask(Task):
  '''Run Fermat Primality Test for a given number.

  Attributes:
      rate (float): A floating point value to control the sampling rate. E.g.
          how many test cases we have to do.
  '''
  rate = 0.6
  num_subtasks = ndb.IntegerProperty(default=0)
  num_finished = ndb.IntegerProperty(default=0)

  def prepare_subtasks(self, tests, prime):
    from webapp2 import get_request
    req = get_request()
    #callback_url = 'http://{host}/handle_fermat_worker'.format(host=req.host)
    callback_url = ''
    child_task_key_format = str(self.key.id()) + '-{}'
    def create_task(i):
      task = FermatPrimalityTestWorkerTask(
        id=child_task_key_format.format(i),
        inputs={'prime': prime, 'base': base},
        parent_task=self.key,
        callback_url='')
      task.callback_function = worker_callback
      return task
    tasks = [create_task(i) for i, base in enumerate(tests)]
    return tasks

  @property
  def subtasks(self):
    child_task_key_format = str(self.key.id()) + '-{}'
    child_task_keys = [ndb.Key(FermatPrimalityTestWorkerTask, child_task_key_format.format(i))
        for i in xrange(0, self.num_subtasks)]
    worker_tasks = ndb.get_multi(child_task_keys)
    for task in worker_tasks:
      if task is None:
        raise StopIteration()
      else:
        yield task

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

    # create the tasks.
    tasks = self.prepare_subtasks(tests, prime)
    ndb.put_multi(tasks)
    self.num_subtasks = len(tasks)
    self.put()

    # put into task queue to run the tests.
    from google.appengine.ext import deferred
    for task in tasks:
      deferred.defer(run_test, task_id=task.key.urlsafe())

  def is_all_subtasks_done(self, func=None, memo=None):
    child_task_key_format = str(self.key.id()) + '-{}'
    reached_the_end = False
    read_base = 0
    index = 0
    while not reached_the_end:
      child_task_keys = [ndb.Key(FermatPrimalityTestWorkerTask, child_task_key_format.format(i))
          for i in xrange(read_base, read_base+1000)]
      worker_tasks = ndb.get_multi(child_task_keys)
      for task in worker_tasks:
        if task is None:
          reached_the_end = True
          break
        else:
          if task.results is None:
            return False, index, None
          else:
            if func and hasattr(func, '__call__'):
              memo = func(memo=memo, task=task, index=index)
          index += 1
      read_base += 1000
    return True, index, memo
