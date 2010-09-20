import logging

class EngineLoader(object):
    """
    This class instantiates decision engine objects based on classname.
    
    """
    
    def __init__(self):
        pass
    
    def load(self, classname):
        """Return instance of specified decision engine"""
        logging.debug("attempting to load Decision Engine '%s'" % classname)
        kls = self._get_class(classname)
        if not kls:
            raise Exception("Cannot find decision engine implementation: '%s'" % classname)
        # Could use zope.interface in the future and check implementedBy here,
        # but it is not actually that helpful.
        
        engine = kls()
        logging.info("Loaded Decision Engine: %s" % str(engine))
        return engine
    
    def _get_class(self, kls):
        parts = kls.split('.')
        module = ".".join(parts[:-1])
        m = __import__( module )
        for comp in parts[1:]:
            m = getattr(m, comp)            
        return m
