package org.openstreetmap.atlas.mutator.configuration.broadcast;

import java.util.Map;

/**
 * @author matthieun
 */
public interface Broadcastable
{
    /**
     * Read the value to broadcast from a definition
     * 
     * @param definition
     *            The definition of the broadcastable value
     * @param configuration
     *            The spark configuration
     * @return The broadcastable variable
     */
    Object read(String definition, Map<String, String> configuration);
}
