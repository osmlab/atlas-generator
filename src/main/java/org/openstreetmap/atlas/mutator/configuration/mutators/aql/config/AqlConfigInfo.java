package org.openstreetmap.atlas.mutator.configuration.mutators.aql.config;

import java.io.Serializable;

/**
 * @author Yazad Khambata
 */
public interface AqlConfigInfo extends Serializable
{
    String getEmbeddedAql();

    String getEmbeddedAqlSignature();
}
