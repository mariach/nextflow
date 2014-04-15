import spock.lang.Specification

/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
class StringMetaClassTest extends Specification {

    def testSplitX() {

        expect:
        new String('x').splitXxx() == 'splitXxx'
        new String('hola').toString() == 'hola'
    }
}
