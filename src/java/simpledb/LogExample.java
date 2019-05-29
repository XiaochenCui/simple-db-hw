package simpledb;

import org.apache.log4j.Logger;

public class LogExample {

    final static Logger logger = Logger.getLogger(LogExample.class);

    public static void main(String[] args) {
        LogExample obj = new LogExample();
        obj.runMe("xiaochen");
    }

    private void runMe(String parameter){

		if(logger.isDebugEnabled()){
			logger.debug("This is debug : " + parameter);
		}

		if(logger.isInfoEnabled()){
			logger.info("This is info : " + parameter);
		}

		logger.warn("This is warn : " + parameter);
		logger.error("This is error : " + parameter);
		logger.fatal("This is fatal : " + parameter);

	}
}
