package com.mgu.kafkaexamples.adapter.external;

import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.PumpStreamHandler;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class ExternalInvoker {

    public static class ExternalInvokerBuilder {

        private final String externalProgram;

        private List<String> arguments = new ArrayList<>();

        public ExternalInvokerBuilder(final String externalProgram) {
            this.externalProgram = externalProgram;
        }

        public ExternalInvokerBuilder withArgument(final String argument) {
            this.arguments.add(argument);
            return this;
        }

        public ExternalInvoker build() {
            final StringBuilder sb = new StringBuilder();
            sb.append(externalProgram);
            if (!arguments.isEmpty()) {
                sb.append(" ");
                sb.append(String.join(" ", arguments));
            }
            return new ExternalInvoker(sb.toString());
        }
    }

    private final String commandTemplate;

    private ExternalInvoker(final String commandTemplate) {
        this.commandTemplate = commandTemplate;
    }

    public String invoke() {
        return invokeWith(Collections.emptyMap());
    }

    public String invokeWith(final Map<String, String> substitutions) {
        final String resolvedCommandTemplate = resolveArguments(substitutions);
        if (hasUnresolvedArguments(resolvedCommandTemplate)) {
            throw new ExternalInvocationException("Unable to execute command due to unresolved arguments.");
        }
        final CommandLine commandLine = CommandLine.parse(resolvedCommandTemplate);
        final DefaultExecutor executor = new DefaultExecutor();
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        final PumpStreamHandler streamHandler = new PumpStreamHandler(outputStream);
        executor.setExitValue(0);
        executor.setStreamHandler(streamHandler);
        try {
            final int exitValue = executor.execute(commandLine);
            if (exitValue != 0) {
                throw new ExternalInvocationException("Unable to execute command: " + resolvedCommandTemplate);
            }
        } catch (IOException e) {
            throw new ExternalInvocationException("Unable to execute command: " + resolvedCommandTemplate, e);
        }
        return outputStream.toString();
    }

    private String resolveArguments(final Map<String, String> substitutions) {
        String template = this.commandTemplate;
        for (String placeholderName : substitutions.keySet()) {
            final String placeholder = "${" + placeholderName + "}";
            template = template.replace(placeholder, substitutions.get(placeholderName));
        }
        return template;
    }

    private boolean hasUnresolvedArguments(final String command) {
        return command.contains("${");
    }

    public static ExternalInvokerBuilder of(final String externalProgram) {
        return new ExternalInvokerBuilder(externalProgram);
    }
}
