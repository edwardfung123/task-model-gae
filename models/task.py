#!/usr/bin/python
# -*- coding: utf-8 -*-

import logging
from google.appengine.ext import ndb

class BaseModel(ndb.Model):
  '''A base model so that every object has a ctime and mtime'''
  ctime = ndb.DateTimeProperty(auto_now_add=True)
  mtime = ndb.DateTimeProperty(auto_now=True)



class Task(BaseModel):
  '''The base task model. Just an abstract class.
  Attributes:
      inputs (ndb.JsonProperty): the data for running the task
      parent_task (ndb.KeyProperty): a ndb key to the parent task. No ancestor
          is used here to avoid large entity group.
      results (ndb.JsonProperty): the result data after running the task. By
          default it is `None` so that the parent task can detemine whether the
          task is finished or not.
      callback_url (ndb.StringProperty): send a POST request to this URL to
          trigger any follow-up actions. Usually, the handler will get the
          parent task and check if all subtasks are done. See also `callback()`
          for more detail about the callback handler.
      callback_fn (ndb.BlobProperty): a serialize callback function and its
          arguments by using the serialize method in Google's deferred library.
          It is used internally. To get/set the function and the arguments, use 
          the property callback_function instead. There are limitions on the
          function to be serialized, please check the source code of the
          deferred library. See
          https://cloud.google.com/appengine/docs/standard/python/refdocs/google.appengine.ext.deferred.deferred . When the callback() is called and callback_fn is set, it 
          will be deserialized and called. Any callback_url will be ignored in
          this case. 
  '''

  inputs = ndb.JsonProperty(default={})
  parent_task = ndb.KeyProperty(default=None, indexed=True)
  results = ndb.JsonProperty(default=None, indexed=True)
  callback_url = ndb.StringProperty(required=True)
  callback_fn = ndb.BlobProperty(default=None)

  def run(self):
    '''Do what the task supposed to do here. This should be overridden by the
    child method.'''
    raise NotImplementedError('implement me!')

  def callback(self):
    '''Send a POST request to the `callback_url` with the following data in the
    request body:

    - task_id: The urlsafe encoded ndb.Key of the current task.

    It is expected that the request handler to get the task and its parent
    task. Use the parent task to query all other tasks and check if there are
    any running task. If not, the parent task can do sth with the data from the
    child tasks and callback if necessary (because of recursive structure).
    
    To customize the data sent to the callback, override the method
    get_callback_data().
    '''
    if self.callback_fn is not None:
      # deserialize it and run the callback
      func, args, kwds = self.callback_function
      if kwds is None:
        kwds = {}
      kwds.update(self.get_callback_data())
      logging.debug('args')
      logging.debug(args)
      logging.debug('kwds')
      logging.debug(kwds)
      return func(*args, **kwds)
    elif self.callback_url:
      from google.appengine.api import urlfetch
      import urllib
      url = self.callback_url
      data = self.get_callback_data()
      payload = urllib.urlencode(data)
      try:
        result = urlfetch.fetch(url, payload=payload, method=urlfetch.POST)
      except urlfetch.Error:
        logging.exception('Caught exception fetching url')
        raise 
      if result.status_code != 200:
        raise ValueError('We have problem to contact the callback ({})...'.format(self.callback_url))
      # everything looks fine... so RIP.
    else:
      logging.warning('Really? It is expected to callback but the callback URL is falsy.')

  def get_callback_data(self):
    '''Prepare data sent to the callback'''
    return {
      'task_id': self.key.urlsafe(),
    }

  @property
  def callback_function(self):
    import pickle
    try:
      func, args, kwds = pickle.loads(self.callback_fn)
    except Exception, e:
      raise PermanentTaskFailure(e)
    else:
      return func, args, kwds

  @callback_function.setter
  def callback_function(self, func, *args, **kwds):
    from google.appengine.ext.deferred import serialize
    self.callback_fn = serialize(func, *args, **kwds)
    return self.callback_fn



class DummyTask(Task):
  '''This is a mock up example'''
  def run(self):
    if self.results is None:
      # do work
      self.results = {
        'is_prime': True,    
      }
      # update myself.
      self.put()
    else:
      logging.debug('we have done the job somehow... should call the callback now.')

    # have to manually call the callback. You might use deferred to do that.
    self.callback()



def runner(fn):
  '''A generic task runner. To workaround the import issue in GAE tasklet.
  You have to create a dummy function and decorate it with this runner
  function.

  Example:

  ```
  @runner
  def run_task():
    pass
  ```
  '''
  from functools import wraps
  @wraps(fn)
  def wrapper(*args, **kwargs):
    logging.debug(args)
    logging.debug(kwargs)
    task_id = kwargs.pop('task_id', None) or args[0]
    task_key = ndb.Key(urlsafe=task_id)
    task = task_key.get()
    if task:
      task.run()
    else:
      logging.error('Failed to get task')
      raise ValueError('No task is mapped to this key')
  return wrapper
