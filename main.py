from typing import Tuple, Dict, List, Iterator
import os

rel_file_path = 'data/words.txt'

words_dict = {}


def prepare_data() -> str:
    cur_dir = os.path.dirname(__file__)
    file_path = os.path.join(cur_dir, rel_file_path)
    with open(file_path, 'r', encoding='utf8') as f:
        contents = f.read()
        return contents


def run(text: str) -> None:
    for word in text.split():
        for key, value in mapper(word):
            shuffler(key, value)

    for key in words_dict.keys():
        word, res_value = reducer(key, words_dict[key])
        print(f'{word}: {res_value}')


def mapper(word: str) -> Iterator[Tuple[str, int]]:
    yield word, 1


def shuffler(word: str, value: int) -> None:
    if word not in words_dict.keys():
        words_dict[word] = [value]
    else:
        word_list = words_dict[word]
        word_list.append(value)
        words_dict[word] = word_list


def reducer(word: str, values: List[int]) -> Tuple[str, int]:
    return word, sum(values)


if __name__ == '__main__':
    data = prepare_data()
    run(data)



