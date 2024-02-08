package yellowdog

import spock.lang.Specification

class YellowDogUtilsTest extends Specification {

    def 'should sanitize invalid name'() {
        when:
        def name = '_myProcess__!__12324567890_12324567890_12324567890_12324567890_12324567890_12324567890_12324567890_12324567890_12324567890__!'
        then:
        YellowDogUtils.sanitizeName(name) == 'my_process_12324567890_12324567890_12324567890_12324567890_1'
    }
}
