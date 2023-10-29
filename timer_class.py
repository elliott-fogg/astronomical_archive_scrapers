from timeit import default_timer as dtimer

class custom_timer():
	def __init__(self):
		self.start = dtimer()

# Call the timer to record a new time, or to reset the timer.
# Allow calling the timer with an incrementer, so that the timer will self-increment,
# and only trigger when it has been called enough times.