package org.openstreetmap.atlas.mutator.configuration.mutators.aql.config;

import org.apache.commons.lang3.Validate;
import org.openstreetmap.atlas.mutator.configuration.util.ClasspathUtil;

/**
 * @author Yazad Khambata
 */
final class AqlConfigHelper
{
    static String loadAqlFromClasspath(final String externalAqlClasspath)
    {
        final String embeddedAql = ClasspathUtil.fromClasspath(externalAqlClasspath);
        Validate.notEmpty(embeddedAql, "embeddedAql is EMPTY for externalAqlClasspath: [%s].",
                externalAqlClasspath);
        return embeddedAql;
    }

    static String loadAqlSignatureFromClasspath(final String externalAqlClasspath)
    {
        final String embeddedAqlSignature = ClasspathUtil
                .fromClasspath(externalAqlClasspath + ".sig");
        Validate.notEmpty(embeddedAqlSignature,
                "embeddedAqlSignature is EMPTY for externalAqlClasspath: [%s].",
                externalAqlClasspath);
        return embeddedAqlSignature;
    }

    private AqlConfigHelper()
    {
    }
}
