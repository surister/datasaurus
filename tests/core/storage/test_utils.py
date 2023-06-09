import pytest

from datasaurus.core.storage.storage import Uri


@pytest.mark.parametrize(
    'input, expected',
    [
        (Uri(path='/tmp'), '/tmp'),
        (Uri(scheme='sqlite', path='home/test'), 'sqlite://home/test'),
        (Uri(scheme='sqlite', path='/home/test'), 'sqlite:////home/test'),
        #                                                ^^^^^
        # This is because of how polar handles backslash escaping, in order for the root path
        # to be valid we have to escape it, from '////home' the first two '//' are from the
        # uri specification (URI = scheme ":" ["//" authority] path ["?" query] ["#" fragment])
        # And the second pair of '//' is from escaping the original '/' from '/home'

        (Uri(scheme='sqlite', user='user', password='password', ), 'sqlite://user:password'),
        (Uri(scheme='sqlite', user='user', password='password', host='localhost'),
         'sqlite://user:password@localhost'),
        (Uri(scheme='sqlite', user='user', password='password', host='localhost', port='2034'),
         'sqlite://user:password@localhost:2034'),
        (Uri(scheme='sqlite', user='user', password='password', host='localhost', port='2034',
             path='postgres'),
         'sqlite://user:password@localhost:2034/postgres')
    ]
)
def test_uri(input, expected):
    assert input.get_uri() == expected
