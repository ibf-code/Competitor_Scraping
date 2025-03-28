import time
import random

USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.3541.1834 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.5284.1732 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.6370.1436 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.5767.1821 Safari/537.36"
]

def random_delay(min_seconds=1, max_seconds=5):
    delay = random.randint(min_seconds, max_seconds)
    print(f"🛌Sleeping for {delay} seconds")
    time.sleep(delay)

def get_random_user_agent():
    return random.choice(USER_AGENTS)