#!/usr/bin/python
# -*- coding: utf-8 -*-

import webapp2
import logging
from google.appengine.ext import ndb
from models.fermat_primality_test_task import FermatPrimalityTestWorkerTask, FermatPrimalityTestTask

class MainHandler(webapp2.RequestHandler):
  def get(self):
    count = FermatPrimalityTestTask.query(FermatPrimalityTestTask.parent_task==None).count()
    self.response.headers['content-type'] = 'text/html'
    new_task_url = 'http://{}/create_task'.format(self.request.host)
    if count == 0:
      self.response.write('There is no task yet. Create one at <a href="{}">here</a>'.format(new_task_url))
    else:
      tasks, cursor, more = FermatPrimalityTestTask.query(
          FermatPrimalityTestTask.parent_task==None
          ).order(-FermatPrimalityTestTask.ctime).fetch_page(1000)
      self.response.write('<p>There are {count} task(s).</p>'.format(count=count))
      self.response.write('<p>Create new one at <a href="{}">here</a>.'.format(new_task_url))
      for t in tasks:
        self.response.write('<p><a href="http://{host}/tasks?task_id={urlsafe_id}">{id}</a></p>'.format(host=self.request.host, id=t.key.id(), urlsafe_id=t.key.urlsafe()))

  def create_task_get(self):
    self.response.headers['content-type'] = 'text/html'
    self.response.write(FORM)

  def create_task_post(self):
    try:
      prime = int(self.request.get('prime'), 10)
    except Exception as e:
      logging.exception(e)
      self.abort(400, 'Invalid input')
    logging.debug(prime)
    inputs = {
        'prime': prime,
    }
    #callback_url = 'http://' + self.request.host + '/handle_task_complete'
    callback_url = ''
    new_task = FermatPrimalityTestTask(inputs=inputs, parent_task=None, results=None, callback_url=callback_url)
    new_task.put()
    new_task.run()
    task_url = 'http://{host}/tasks?task_id={urlsafe_id}'.format(host=self.request.host, urlsafe_id=new_task.key.urlsafe())
    self.response.headers['content-type'] = 'text/html'
    self.response.write(NEW_TASK.format(task_id=new_task.key.id(), task_url=task_url))

  def handle_fermat_callback(self):
    ''' The callback handler for the FermatPrimalityTestTask '''
    task_id = self.request.get('task_id', '').strip()
    task_key = ndb.Key(urlsafe=task_id)
    task = task_key.get()
    if task is None:
      self.abort(404, 'The task does not exist. really?')
    logging.debug(task.inputs)
    logging.debug(task.results)

  @ndb.transactional(retries=10)
  def update_parent_task_finished(self, parent_task_key, val):
    parent_task = parent_task_key.get()
    parent_task.num_finished += val
    parent_task.put()
    return True

  def handle_fermat_worker_callback(self):
    task_id = self.request.get('task_id', '').strip()
    task_key = ndb.Key(urlsafe=task_id)
    task = task_key.get()
    if task is None:
      self.abort(404, 'The task does not exist. really?')
    logging.debug(task.inputs)
    logging.debug(task.results)
    # check if all other workers are done too.
    parent_task = task.parent_task.get()
    if parent_task is None:
      self.abort(404, 'The parent task does not exist.')

    # If we are lucky enough, the checking can be run in multiple times in
    # parallel. Perhaps it is important to know it. In this case, we don't
    # have any problem running this multiple times. just silly.
    self.update_parent_task_finished(task.parent_task, 1)

    parent_task = task.parent_task.get()

    if parent_task.num_finished == parent_task.num_subtasks:
      is_prime = True
      for task in parent_task.subtasks:
        if task.results['is_prime'] is False:
          is_prime = False
          break
      parent_task.results = {'is_prime': is_prime}
      parent_task.put()
      parent_task.callback()
    else:
      # fine then. we wait for another callback.
      pass
    
  def show_task(self):
    task_id = self.request.get('task_id', '').strip()
    task_key = ndb.Key(urlsafe=task_id)
    task = task_key.get()
    if task is None:
      self.abort(404, 'The task does not exist. really?')
    self.response.headers['content-type'] = 'text/html'
    logging.debug(task.key.id())
    subtasks, cursor, more = FermatPrimalityTestWorkerTask.query(
        FermatPrimalityTestWorkerTask.parent_task==task_key).fetch_page(1000)
    subtasks_htmls = [TASK_DETAIL.format(task=subtask, subtasks_html=None, total=None, done=None, percent=None) for subtask in subtasks]
    subtasks_html = '<hr>'.join(subtasks_htmls)
    total = len(subtasks)
    done = len([True for subtask in subtasks if subtask.results is not None])
    percent = '{:.2f}'.format(100 * float(done) / total)
    self.response.write(TASK_DETAIL.format(task=task, subtasks_html=subtasks_html, total=total, done=done, percent=percent))
    


app = webapp2.WSGIApplication([
  webapp2.Route('/create_task', handler='main.MainHandler:create_task_get', methods=['GET']),
  webapp2.Route('/create_task', handler='main.MainHandler:create_task_post', methods=['POST']),
  webapp2.Route('/handle_task_complete', handler='main.MainHandler:handle_fermat_callback', methods=['POST']),
  webapp2.Route('/handle_fermat_worker', handler='main.MainHandler:handle_fermat_worker_callback', methods=['POST']),
  webapp2.Route('/tasks', handler='main.MainHandler:show_task', methods=['GET']),
  ('/*', MainHandler),
  ], debug=True)



# Some template stings for HTML...



FORM = '''<!DOCTYPE HTML>
<html>
  <head>
    <style>
      input, button {
        width: 100%;
        box-sizing: border-box;
      }
    </style>
  </head>
  <body>
    <h1>Create a new task to test a number is prime.</h1>
    <form action="/create_task" method="POST">
      <input type="number" name="prime" placeholder="The number to be tested." required="required"/>
      <button type="submit">Send</button>
    </form>
  </body>
</html>'''

NEW_TASK = '''<!DOCTYPE HTML>
<html>
  <head>
    <style>
    </style>
  </head>
  <body>
    <p>A new task with ID = {task_id} is created.</p>
    <p>You can check the task progress in <a href="{task_url}">here</a>.</p>
  </body>
</html>'''

TASK_DETAIL = '''<!DOCTYPE HTML>
<html>
  <head>
    <style>
    </style>
  </head>
  <body>
    <p>Task: {task.key}</p>
    <h2>Inputs</h2>
    <pre>{task.inputs}</pre>
    <h2>Results</h2>
    <pre>{task.results}</pre>
    <h2>Subtasks</h2>
    <h3>Progress</h3>
    <p>{done} / {total} = {percent}%</p>
    <div style="margin-left: 50px">
      {subtasks_html}
    </div>
  </body>
</html>'''
