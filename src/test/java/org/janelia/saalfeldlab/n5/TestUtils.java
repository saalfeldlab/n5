package org.janelia.saalfeldlab.n5;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import static org.junit.Assume.assumeTrue;

public class TestUtils {



    //TODO: Migrate to junit5. It has built in support for this without a custom rule

    /* Skips the annotated test if not on Windows */
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.METHOD)
    public @interface WindowsOnly {

        class Rule implements TestRule {

            private static final boolean isWindows = System.getProperty("os.name").toLowerCase().startsWith("win");

            @Override
            public Statement apply(final Statement base, final Description description) {

                return new Statement() {

                    @Override
                    public void evaluate() throws Throwable {

                        if (description.getAnnotation(WindowsOnly.class) != null)
                            assumeTrue("Windows only", isWindows);
                        base.evaluate();
                    }
                };
            }
        }

    }

}
