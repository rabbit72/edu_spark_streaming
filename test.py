import pytest
from producer import batch_generator


@pytest.mark.parametrize("chunksize, input, expected_output", [
    (2, [1, 2, 3, 4, 5], [[1, 2], [3, 4], [5]]),
    (2, [1, 2, 3, 4], [[1, 2], [3, 4]]),
    (4, [1, 2, 3, 4], [[1, 2, 3, 4]]),
    (5, [1, 2, 3, 4], [[1, 2, 3, 4]]),

])
def test_batch_generator(chunksize, input, expected_output):
    iterable: iter = iter(input)
    assert list(batch_generator(iterable, chunksize)) == expected_output
