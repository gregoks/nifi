package org.apache.nifi.processors.kudu;


import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.processor.util.StandardValidators;

public class ConfigFilesValidator implements Validator {

    @Override
    public ValidationResult validate(final String subject, final String value, final ValidationContext context) {
        final String[] filenames = value.split(",");
        for (final String filename : filenames) {
            final ValidationResult result = StandardValidators.FILE_EXISTS_VALIDATOR.validate(subject, filename.trim(), context);
            if (!result.isValid()) {
                return result;
            }
        }

        return new ValidationResult.Builder().subject(subject).input(value).valid(true).build();
    }
}