"""
A bare bones Ion capability container.

This is a tac file, a Python file for configuring a Twisted application.

Follow this example when making your own tac files.

Run this from within an ion environment directory:

    twistd -ny res/tacs/empty_container.tac

@see http://twistedmatrix.com/documents/current/core/howto/application.html
"""
from twisted.application import service

from ion.core.cc import service as ion_service
from ion.core import ioninit

def makeContainerService():
    """
    This is boiler plate code.
    """
    config = ion_service.Options()
    config['no_shell'] = True #Don't need the shell
    config['script'] = 'res/apps/ccagent.app' #Eww, this looks ugly!
    container = ion_service.CapabilityContainer(config)
    return container

application = service.Application('ion')

container = makeContainerService()
container.setServiceParent(service.IServiceCollection(application))
