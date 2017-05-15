/**
 * Exceptions
 */
enum CalculatorErrorCode {
    UNKNOWN_ERROR = 0,
    DATABASE_ERROR = 1,
    TOO_BUSY_ERROR = 2,
}

exception CalculatorUserException {
   1: required CalculatorErrorCode error_code,
   2: required string error_name,
   3: optional string message,
}

exception CalculatorSystemException {
   1: required CalculatorErrorCode error_code,
   2: required string error_name,
   3: optional string message,
}

exception CalculatorUnknownException {
   1: required CalculatorErrorCode error_code,
   2: required string error_name,
   3: required string message,
}

/**
 * API
 */
service CalculatorService {
    bool ping()
        throws (1: CalculatorUserException user_exception,
                2: CalculatorSystemException system_exception,
                3: CalculatorUnknownException unknown_exception),

    i32 add(1:i32 num1, 2:i32 num2)
        throws (1: CalculatorUserException user_exception,
                2: CalculatorSystemException system_exception,
                3: CalculatorUnknownException unknown_exception),
}
