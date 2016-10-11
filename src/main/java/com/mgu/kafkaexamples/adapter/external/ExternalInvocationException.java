package com.mgu.kafkaexamples.adapter.external;

public class ExternalInvocationException extends RuntimeException {

    private static final String ERROR_MESSAGE = "Unable to execute command: %s";

    public ExternalInvocationException(final String command) {
        super(String.format(ERROR_MESSAGE, command));
    }

    public ExternalInvocationException(final Throwable cause) {
        super(cause);
    }

    public ExternalInvocationException(final String command, final Throwable cause) {
        super(String.format(ERROR_MESSAGE, command), cause);
    }
}
