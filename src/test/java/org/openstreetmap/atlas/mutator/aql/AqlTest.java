package org.openstreetmap.atlas.mutator.aql;

import java.util.UUID;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.openstreetmap.atlas.geography.atlas.Atlas;
import org.openstreetmap.atlas.geography.atlas.dsl.authentication.impl.SHA512HMACAuthenticatorImpl;
import org.openstreetmap.atlas.geography.atlas.dsl.engine.QueryExecutor;
import org.openstreetmap.atlas.geography.atlas.dsl.engine.impl.SecureQueryExecutorImpl;
import org.openstreetmap.atlas.geography.atlas.dsl.query.result.Result;
import org.openstreetmap.atlas.mutator.configuration.util.ClasspathUtil;
import org.openstreetmap.atlas.mutator.configuration.util.consts.AqlConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Yazad Khambata
 */
public class AqlTest
{
    public static final String AQL_FILE = "/aql-files/testUpdate1.aql";
    private static final Logger log = LoggerFactory.getLogger(AqlTest.class);
    private static final String QUERY = ClasspathUtil.fromClasspath(AQL_FILE);
    private static QueryExecutor queryExecutor;
    @Rule
    public final AqlTestRule rule = new AqlTestRule();

    @BeforeClass
    public static void setup()
    {
        System.setProperty(AqlConstants.SYSTEM_KEY, UUID.randomUUID().toString());
        queryExecutor = new SecureQueryExecutorImpl();
    }

    @Test
    public void sanity()
    {
        final Atlas atlas = this.rule.getAtlas();
        final Result result = queryExecutor.exec(atlas, QUERY,
                new SHA512HMACAuthenticatorImpl(System.getProperty(AqlConstants.SYSTEM_KEY))
                        .sign(QUERY));

        log.info("Ids: {}.", result.getRelevantIdentifiers());

        Assert.assertEquals(3, result.getRelevantIdentifiers().size());
    }
}
