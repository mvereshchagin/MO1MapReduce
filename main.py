from typing import Tuple, Dict, List, Iterator
import os
import re
from threading import Thread
import time

input_dir = 'data/'
mapped_words = []
exclude_words = 'в на по у под над к с и а для не при из'.split()


def run() -> None:
    start_time = time.time()

    cur_dir = os.path.dirname(__file__)

    dir_path = os.path.join(cur_dir, input_dir)

    threads = []
    for file_path in os.listdir(dir_path):
        full_file_path = os.path.join(dir_path, file_path)
        with open(full_file_path, 'r', encoding='utf8') as f:
            content = f.read()
            thread = Thread(target=mapper, args=(content, ))
            threads.append(thread)
            thread.start()

    for thread in threads:
        thread.join()

    results = []
    for word, lst in shuffler():
        results.append(reducer(word, lst))

    for value, word in sorted(results):
        print(f'{-value} {word}')

    print(f'multithreading time: {time.time() - start_time}')


def mapper(text: str) -> None:
    for word in re.findall(r'[\w]+', text):  # text.split():
        if word.lower() not in exclude_words:
            mapped_words.append((word.lower(), 1))


def shuffler() -> Iterator[Tuple[str, List[int]]]:
    new_words = sorted(mapped_words)

    buffer = []
    prev_word = None
    for word, value in new_words:
        if prev_word == word:
            buffer.append(value)
        else:
            if prev_word is not None:
                yield prev_word, buffer
            buffer = [value]
        prev_word = word

    if buffer:
        yield prev_word, buffer


def reducer(word: str, values: List[int]) -> Tuple[int, str]:
    return -sum(values), word


if __name__ == '__main__':
    run()



