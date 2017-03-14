# Introdction

This is an experiment with the Task Queue in Google Appengine.

## Background

We often need to batch process something (e.g. import objects with CSV).
However, this is likely to exceed the 60 seconds time limit of the Frontend
instance. To overcome, Google suggests to use Task Queue to do long running
task. It is also better for UX because the browser does not have to wait
forever until the task completes. The task queue works very well but it does
not have the features I need/want.
