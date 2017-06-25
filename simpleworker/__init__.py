import os

from json import dumps, loads
from threading import Condition, Thread
from time import sleep


__all__ = ('Worker', 'WorkerManager', 'WorkerQueue')


class WorkerQueue(object):
    def __init__(self, path):
        self.path = path
        self._runnings = []
        self._queue = []
        self._lock = Condition()

        self._load()

    def __repr__(self):
        return '<WorkerQueue runnings=%d jobs=%d>' % (len(self._runnings), len(self._jobs))

    def __len__(self):
        return len(self._jobs)

    def to_dict(self):
        with self._lock:
            runnings = self._runnings
            jobs = self._jobs
            return {
                'runnings': runnings,
                'runnings_count': len(runnings),
                'jobs': jobs,
                'jobs_count': len(jobs),
            }

    @property
    def _jobs(self):
        return [job for job in self._queue if job is not None]

    def _load(self):
        try:
            with open(self.path) as f:
                for job in f:
                    self.put(loads(job))
        except FileNotFoundError:
            pass

    def _save(self):
        with open(self.path, 'w') as f:
            jobs = self._runnings + self._jobs
            for job in jobs:
                print(dumps(job), file=f)

    def put(self, job):
        with self._lock:
            if job in self._runnings or job in self._queue:
                return
            self._queue.append(job)
            self._save()
            self._lock.notify()

    def get(self):
        with self._lock:
            while not self._queue:
                self._lock.wait()
            job = self._queue.pop(0)
            if job is not None:
                self._runnings.append(job)
            return job

    def done(self, job):
        with self._lock:
            self._runnings.remove(job)
            self._save()

    def fail(self, job):
        with self._lock:
            self._runnings.remove(job)
            self._queue.append(job)
            self._save()

    def stop_workers(self, n=1):
        with self._lock:
            for i in range(n):
                self._queue.insert(0, None)
                self._lock.notify()


class Worker(object):
    def __init__(self, name, num_workers, function, path):
        self.name = name
        self.num_workers = num_workers
        self._function = function
        self._jobs = WorkerQueue(path)
        self._workers = []
        self._count = 0

    def __repr__(self):
        return '<Worker:%s workers=%d queue=%d>' % (self.name, len(self._workers), len(self._jobs))

    def to_dict(self):
        return {
            'workers_count': self.num_workers,
            'jobs': self._jobs.to_dict(),
        }

    def check(self):
        self._workers = [worker for worker in self._workers if worker.is_alive()]
        while len(self._workers) < self.num_workers:
            name = '%s-%d' % (self.name, self._count)
            self._count += 1
            worker = Thread(name=name, target=self._function, args=(name, self._jobs,))
            worker.start()
            self._workers.append(worker)
        n = len(self._workers) - self.num_workers
        if n > 0:
            self._jobs.stop_workers(n)

    def signal_to_stop(self):
        self._jobs.stop_workers(len(self._workers))

    def wait_jobs(self):
        for worker in self._workers:
            worker.join()

    def add_job(self, job):
        self._jobs.put(job)


class WorkerManager(object):
    def __init__(self, path, name='simpleworker'):
        self._path = path
        self.name = name
        self._running = False
        self._lock = Condition()
        self._workers = {}
        self._thread = None

        os.makedirs(self._path, exist_ok=True)

    def __repr__(self):
        return '<WorkerManager:%s workers=%d>' % (self.name, len(self._workers))

    def to_dict(self):
        return {name: worker.to_dict() for name, worker in self._workers.items()}

    def start(self):
        if not self._running:
            self._running = True
            self._thread = Thread(name=self.name, target=self._run)
            self._thread.start()

    def stop(self):
        self._running = False
        with self._lock:
            for worker in self._workers.values():
                worker.signal_to_stop()
            for worker in self._workers.values():
                worker.wait_jobs()
        self._thread.join()

    def _run(self):
        while self._running:
            with self._lock:
                for worker in self._workers.values():
                    worker.check()
            sleep(30)

    def _add_worker(self, name, num_workers, function):
        self._workers[name] = Worker(name, num_workers, function, os.path.join(self._path, name))

    def register(self, name=None, num_workers=1, function=None):
        if function is not None:
            self._add_worker(name, num_workers, function)
            return

        def wrapper(function):
            if name is None:
                job_name = function.__name__
            else:
                job_name = name
            self._add_worker(job_name, num_workers, function)
            return function
        return wrapper

    def add_job(self, name, job):
        self._workers[name].add_job(job)
