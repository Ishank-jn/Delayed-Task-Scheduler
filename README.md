# Delayed-Task-Scheduler
Task scheduler: List of runnables that are executed after t ms by consumers. 

Rescheduling: If a task is not yet due when the consumer tries to execute it, it's rescheduled back onto the queue to ensure correct timing.

Code clarity: The code is further formatted and commented for better readability and maintainability.

Efficiency: While heapq.heappush(self.queue, next_task) re-adds a task that's not due yet, its impact on performance is negligible considering the typical use cases of task schedulers.

Error handling: The _consumer_loop now includes a try-except block to gracefully handle exceptions during task execution, preventing the scheduler from crashing.

Executables can be a lambda function or can be replaced by a pickle file.
