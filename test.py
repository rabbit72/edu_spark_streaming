import pytest
from producer import batch_generator


@pytest.mark.parametrize("chunksize, input, expected_output", [
    (2, [1, 2, 3, 4, 5], [[1, 2], [3, 4], [5]]),
    (2, [1, 2, 3, 4], [[1, 2], [3, 4]]),
    (4, [1, 2, 3, 4], [[1, 2, 3, 4]]),
    (5, [1, 2, 3, 4], [[1, 2, 3, 4]]),
    (1, [], []),

])
def test_batch_generator(chunksize, input, expected_output):
    iterable: iter = iter(input)
    assert list(batch_generator(iterable, chunksize)) == expected_output


@pytest.mark.parametrize("chunksize", [0, -1, 5.35, 'error'])
def test_batch_generator_value_error(chunksize):
    iterable: iter = iter([1, 2, 3])
    with pytest.raises(ValueError):
        list(batch_generator(iterable, chunksize))
