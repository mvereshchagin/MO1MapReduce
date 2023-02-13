from typing import Tuple, Dict, List, Iterator
import os
import re
import multiprocessing as mp
import time

input_dir = 'data/'
exclude_words = 'в на по у под над к с и а для не при из'.split()


def run() -> None:

    start_time = time.time()

    with mp.Pool() as pool:
        mapped_words = []
        for words in pool.map(worker, read()):
            mapper_all(mapped_words, words)

        results = []
        for word, lst in shuffler(mapped_words):
            results.append(reducer_all(word, lst))

        for count, word in sorted(results):
            print(f'{-count} {word}')

    print(f'multiprocessing time: {time.time() - start_time}')


def read() -> Iterator[str]:
    cur_dir = os.path.dirname(__file__)

    dir_path = os.path.join(cur_dir, input_dir)

    for file_path in os.listdir(dir_path):
        full_file_path = os.path.join(dir_path, file_path)
        with open(full_file_path, 'r', encoding='utf8') as f:
            yield f.read()


def worker(text: str) -> List[Tuple[str, int]]:
    mapped_words = mapper(text)

    results = []
    for word, lst in shuffler(mapped_words):
        results.append(reducer(word, lst))

    return results


def mapper(text: str) -> List[Tuple[str, int]]:
    mapped_words = []
    for word in re.findall(r'[\w]+', text):  # text.split():
        if word.lower() not in exclude_words:
            mapped_words.append((word.lower(), 1))
    return mapped_words


def shuffler(mapped_words: List[Tuple[str, int]]) -> Iterator[Tuple[str, List[int]]]:
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


def reducer(word: str, values: List[int]) -> Tuple[str, int]:
    return word, sum(values)


def mapper_all(mapped_words: List[Tuple[str, int]], words: List[Tuple[str, int]]) -> List[Tuple[str, int]]:
    for word, value in words:  # text.split():
        mapped_words.append((word, value))
    return mapped_words


def reducer_all(word: str, values: List[int]) -> Tuple[int, str]:
    return -sum(values), word


if __name__ == '__main__':
    run()



