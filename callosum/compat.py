'''
Compatibility functions for multiple Python versions and stdlib differences.
'''

import asyncio

__all__ = ('current_loop', )


# Python 3.7 offers safer "get_running_loop" API which raises explicit exceptions
# when there is no event loop attached to the current thread.
if hasattr(asyncio, 'get_running_loop'):
    current_loop = asyncio.get_running_loop
else:
    current_loop = asyncio.get_event_loop
