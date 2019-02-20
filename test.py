import pytest
from producer import chunk_generator


@pytest.mark.parametrize("chunksize, input_data, expected_output", [
    (2, [1, 2, 3, 4, 5], [[1, 2], [3, 4], [5]]),
    (2, [1, 2, 3, 4], [[1, 2], [3, 4]]),
    (4, [1, 2, 3, 4], [[1, 2, 3, 4]]),
    (5, [1, 2, 3, 4], [[1, 2, 3, 4]]),
    (1, [], []),

])
def test_chunk_generator(chunksize, input_data, expected_output):
    iterable: iter = iter(input_data)
    assert list(chunk_generator(iterable, chunksize)) == expected_output


@pytest.mark.parametrize("chunksize", [0, -1, 5.35, 'error'])
def test_chunk_generator_value_error(chunksize):
    iterable: iter = iter([1, 2, 3])
    with pytest.raises(ValueError):
        list(chunk_generator(iterable, chunksize))
