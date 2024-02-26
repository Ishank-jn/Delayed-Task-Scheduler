# Multiple producers and consumers: You can add multiple queues and start separate 
# threads for each producer and consumer.
# Separate start for producers and consumers: You can control when each group starts processing tasks.
# Individual queues: Each consumer has its own queue, allowing for independent processing and prioritization.
import threading
import time
import heapq

class Task:
    def __init__(self, runnable, delay_ms):
        self.runnable = runnable
        self.timestamp_ms = time.time() * 1000 + delay_ms  # Convert to ms

class SchedulerQueue:
    def __init__(self):
        self.queue = []
        self.stop_flag = False

class Consumer(threading.Thread):
    def __init__(self, scheduler_queue):
        super().__init__()
        self.queue = scheduler_queue

    def run(self):
        while not self.queue.stop_flag:
            if self.queue.queue:
                next_task = heapq.heappop(self.queue.queue)
                if next_task.timestamp_ms <= time.time() * 1000:
                    try:
                        next_task.runnable()
                    except Exception as e:
                        print(f"Error running task: {e}")
                else:
                    next_task.timestamp_ms+=25 # Add 25 ms delay and reschedule
                    heapq.heappush(self.queue.queue, next_task)  # Reschedule if not due yet
            time.sleep(0.1)  # Prevent busy waiting

class Producer(threading.Thread):
    def __init__(self, scheduler_queue):
        super().__init__()
        self.queue = scheduler_queue

    def run(self):
        while not self.queue.stop_flag:
            # Simulate tasks being added to the queue
            # (replace this with your actual task generation logic)
            task = Task(lambda: print(f"Running task at {time.time() * 1000:.2f} ms"), 1000)
            self.queue.add_task(task)
            time.sleep(1)  # Simulate task generation interval

class TaskScheduler:
    def __init__(self):
        self.stop_flag = False
        self.queues = {}  # Map consumer names to their queues

    def add_queue(self, consumer_name):
        self.queues[consumer_name] = SchedulerQueue()

    def add_task(self, consumer_name, runnable, delay_ms):
        if consumer_name not in self.queues:
            raise ValueError(f"Consumer '{consumer_name}' not found")
        task = Task(runnable, delay_ms)
        self.queues[consumer_name].add_task(task)

    def start_producers(self, producer_names):
        for name in producer_names:
            if name not in self.queues:
                raise ValueError(f"Queue for producer '{name}' not found")
            producer = Producer(self.queues[name])
            producer.start()

    def start_consumers(self, consumer_names):
        for name in consumer_names:
            if name not in self.queues:
                raise ValueError(f"Queue for consumer '{name}' not found")
            consumer = Consumer(self.queues[name])
            consumer.start()

    def stop(self):
        self.stop_flag = True
        for queue in self.queues.values():
            queue.stop_flag = True
        for thread in threading.enumerate():
            if isinstance(thread, (Producer, Consumer)):
                thread.join()

# Example usage
scheduler = TaskScheduler()
scheduler.add_queue("consumer1")
scheduler.add_queue("consumer2")
scheduler.add_task("consumer1", lambda: print("Task 1 for consumer1"), 2000)
scheduler.add_task("consumer2", lambda: print("Task 1 for consumer2"), 3000)

scheduler.start_producers(["consumer1", "consumer2"])
scheduler.start_consumers(["consumer1", "consumer2"])
time.sleep(5)  # Allow tasks to run for a while
scheduler.stop()