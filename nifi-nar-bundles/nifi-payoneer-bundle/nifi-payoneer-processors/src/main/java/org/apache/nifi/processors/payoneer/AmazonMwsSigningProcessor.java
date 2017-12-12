package org.apache.nifi.processors.payoneer;

import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.AttributeExpression;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.hbase.HBaseClientService;
import org.apache.nifi.processor.*;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Map.Entry;
import java.util.regex.Pattern;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import org.apache.commons.codec.binary.Base64;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import static org.apache.nifi.processor.util.StandardValidators.NON_BLANK_VALIDATOR;

@EventDriven
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"amazon", "mws","signature"})
@CapabilityDescription("Generates a signed url for use with Amazon MWS")
public class AmazonMwsSigningProcessor extends AbstractProcessor {
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("A FlowFile is routed to this relationship if a valid signature cannot be calculated")
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("A FlowFile is routed to this relationship if a valid signature was calculated")
            .build();


    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    private static final String CHARACTER_ENCODING = "UTF-8";
    final static String ALGORITHM = "HmacSHA256";

    protected static final PropertyDescriptor SECRET_KEY = new PropertyDescriptor.Builder()
            .name("Secret Key")
            .description("The amazon API secret key")
            .required(true)
            .expressionLanguageSupported(true)
            .sensitive(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    protected static final PropertyDescriptor AUTH_TOKEN = new PropertyDescriptor.Builder()
            .name("Auth token")
            .description("MWS Auth token")
            .required(true)
            .expressionLanguageSupported(true)
            .sensitive(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();


    protected static final PropertyDescriptor ACCESS_KEY = new PropertyDescriptor.Builder()
            .name("Access key")
            .description("MWS Access key")
            .required(true)
            .expressionLanguageSupported(true)
            .sensitive(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    protected static final PropertyDescriptor SERVICE_URL = new PropertyDescriptor.Builder()
            .name("Service URL")
            .description("The mws service url")
            .required(true)
            .expressionLanguageSupported(true)
            .defaultValue("https://mws.amazonservices.com/")
            .addValidator(StandardValidators.URL_VALIDATOR)
            .build();

    protected static final PropertyDescriptor ACTION = new PropertyDescriptor.Builder()
            .name("Action")
            .description("The amazon API action")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    protected static final PropertyDescriptor VERSION = new PropertyDescriptor.Builder()
            .name("Version")
            .description("The API Call version")
            .required(true)
            .defaultValue("2009-01-01")
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.createRegexMatchingValidator(Pattern.compile("2[0-9]{3}(-[0-9]{2}){2}") ))
            .build();

    protected static final PropertyDescriptor SELLER_ID = new PropertyDescriptor.Builder()
            .name("Seller Id")
            .description("The amazon mws seller id")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();


    protected static final PropertyDescriptor OUTPUT_ATTRIBUTE = new PropertyDescriptor.Builder()
            .name("Output attribute")
            .description("The attribute to store the signature result to")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.ATTRIBUTE_KEY_VALIDATOR)
            .build();

    protected static final PropertyDescriptor OUTPUT_URL_ATTRIBUTE = new PropertyDescriptor.Builder()
            .name("Output URL attribute")
            .description("The attribute to store the full url result to")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.ATTRIBUTE_KEY_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROP_METHOD = new PropertyDescriptor.Builder()
            .name("HTTP Method")
            .description("HTTP request method (GET, POST, PUT, PATCH, DELETE, HEAD, OPTIONS). Arbitrary methods are also supported. "
                    + "Methods other than POST, PUT and PATCH will be sent without a message body.")
            .required(true)
            .defaultValue("POST")
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.STRING))
            .build();


    final TimeZone tz = TimeZone.getTimeZone("UTC");
    final DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm'Z'"); // Quoted "Z" to indicate UTC, no timezone offset

    @Override
    protected void init(final ProcessorInitializationContext context) {

        df.setTimeZone(tz);
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();

        descriptors.add(SECRET_KEY);
        descriptors.add(SERVICE_URL);
        descriptors.add(PROP_METHOD);
        descriptors.add(ACTION);
        descriptors.add(VERSION);

        descriptors.add(AUTH_TOKEN);
        descriptors.add(SELLER_ID);
        descriptors.add(ACCESS_KEY);
        descriptors.add(OUTPUT_ATTRIBUTE);

        descriptors.add(OUTPUT_URL_ATTRIBUTE);


        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();

        relationships.add(REL_FAILURE);
        relationships.add(REL_SUCCESS);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }


    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .description("Extra field")
                .dynamic(true)
                .required(false)
                .expressionLanguageSupported(true)
                .addValidator(NON_BLANK_VALIDATOR)
                .build();
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {

        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }
        // Change this secret key to yours
        final String secretKey = context.getProperty(SECRET_KEY).evaluateAttributeExpressions(flowFile).getValue();

        // Use the endpoint for your marketplace
        final String serviceUrl = context.getProperty(SERVICE_URL).evaluateAttributeExpressions(flowFile).getValue();
        final String action = context.getProperty(ACTION).evaluateAttributeExpressions(flowFile).getValue();
        final String auth_token = context.getProperty(AUTH_TOKEN).evaluateAttributeExpressions(flowFile).getValue();
        final String seller_id= context.getProperty(SELLER_ID).evaluateAttributeExpressions(flowFile).getValue();
        final String access_key= context.getProperty(ACCESS_KEY).evaluateAttributeExpressions(flowFile).getValue();
        final String output_attribute= context.getProperty(OUTPUT_ATTRIBUTE).evaluateAttributeExpressions(flowFile).getValue();
        final String output_url_attribute= context.getProperty(OUTPUT_URL_ATTRIBUTE).evaluateAttributeExpressions(flowFile).getValue();
final String version = context.getProperty(VERSION).evaluateAttributeExpressions(flowFile).getValue();
        final String method= context.getProperty(PROP_METHOD).evaluateAttributeExpressions(flowFile).getValue();

        // Create set of parameters needed and store in a map
        HashMap<String, String> parameters = new HashMap<String,String>();

        // Add required parameters. Change these as needed.

        parameters.put("Action", urlEncode(action));
        parameters.put("MWSAuthToken", urlEncode(auth_token));
        parameters.put("SellerId", urlEncode(seller_id));
        parameters.put("AWSAccessKeyId", urlEncode(access_key));

        parameters.put("SignatureMethod", urlEncode(ALGORITHM));
        parameters.put("SignatureVersion", urlEncode("2"));

        parameters.put("Timestamp", urlEncode(df.format(new Date())));
        parameters.put("Version", urlEncode(version));

        for(Map.Entry<PropertyDescriptor,String> entry : context.getProperties().entrySet()) {
            if (!entry.getKey().isDynamic()) continue;
            parameters.put(entry.getKey().getName(),
                    urlEncode(context.getProperty(entry.getKey().getName()).evaluateAttributeExpressions(flowFile).getValue()));
        }

        // Format the parameters as they will appear in final format
        // (without the signature parameter)
        String formattedParameters = null;
        try {
            formattedParameters = calculateStringToSignV2(method,parameters,
                    serviceUrl);

        String signature = sign(formattedParameters, secretKey);

        // Add signature to the parameters and display final results
        parameters.put("Signature", urlEncode(signature));
            flowFile = session.putAttribute(flowFile,output_attribute,urlEncode(signature));



        String signedUrl = generateUrl(parameters,serviceUrl);
        flowFile = session.putAttribute(flowFile,output_url_attribute,signedUrl);
        session.transfer(flowFile,REL_SUCCESS);



        } catch (Exception e) {
            getLogger().error("Could not create signature {}",new Object[]{e},e);
            session.transfer(flowFile,REL_FAILURE);
        }

    }

    /* If Signature Version is 2, string to sign is based on following:
        *
        *    1. The HTTP Request Method followed by an ASCII newline (%0A)
        *
        *    2. The HTTP Host header in the form of lowercase host,
        *       followed by an ASCII newline.
        *
        *    3. The URL encoded HTTP absolute path component of the URI
        *       (up to but not including the query string parameters);
        *       if this is empty use a forward '/'. This parameter is followed
        *       by an ASCII newline.
        *
        *    4. The concatenation of all query string components (names and
        *       values) as UTF-8 characters which are URL encoded as per RFC
        *       3986 (hex characters MUST be uppercase), sorted using
        *       lexicographic byte ordering. Parameter names are separated from
        *       their values by the '=' character (ASCII character 61), even if
        *       the value is empty. Pairs of parameter and values are separated
        *       by the '&' character (ASCII code 38).
        *
        */
    private static String calculateStringToSignV2(String method,
            Map<String, String> parameters, String serviceUrl)
            throws SignatureException, URISyntaxException {

        // Set endpoint value
        URI endpoint = new URI(serviceUrl.toLowerCase());

        // Create flattened (String) representation
        StringBuilder data = new StringBuilder();
        data.append(method).append("\n");
        data.append(endpoint.getHost());
        //data.append("\n/");
        data.append("\n" + endpoint.getPath()) ;
        data.append("\n");
        return buildQueryString(parameters, data);


    }
    private static String generateUrl(Map<String, String> parameters, String serviceUrl)
            throws SignatureException, URISyntaxException {

        // Set endpoint value
        URI endpoint = new URI(serviceUrl.toLowerCase());
        // Create flattened (String) representation
        StringBuilder data = new StringBuilder("https://" + endpoint.getHost() +"/" + endpoint.getPath() +"?");
        return buildQueryString(parameters, data);

    }

    private static String buildQueryString(Map<String, String> parameters, StringBuilder data) {
        // Sort the parameters alphabetically by storing
        // in TreeMap structure
        Map<String, String> sorted = new TreeMap<String, String>();
        sorted.putAll(parameters);

        Iterator<Entry<String, String>> pairs =
                sorted.entrySet().iterator();
        while (pairs.hasNext()) {
            Entry<String, String> pair = pairs.next();
            if (pair.getValue() != null) {
                data.append( pair.getKey() + "=" + pair.getValue());
            }
            else {
                data.append( pair.getKey() + "=");
            }

            // Delimit parameters with ampersand (&)
            if (pairs.hasNext()) {
                data.append( "&");
            }
        }

        return data.toString();
    }

    /*
     * Sign the text with the given secret key and convert to base64
     */
    private static String sign(String data, String secretKey)
            throws NoSuchAlgorithmException, InvalidKeyException,
            IllegalStateException, UnsupportedEncodingException {

        Mac mac = Mac.getInstance(ALGORITHM);
        mac.init(new SecretKeySpec(secretKey.getBytes(CHARACTER_ENCODING),
                ALGORITHM));
        byte[] signature = mac.doFinal(data.getBytes(CHARACTER_ENCODING));
        String signatureBase64 = new String(Base64.encodeBase64(signature),
                CHARACTER_ENCODING);

        return new String(signatureBase64);
    }

    private static String urlEncode(String rawValue) {
        String value = (rawValue == null) ? "" : rawValue;
        String encoded = null;

        try {
            encoded = URLEncoder.encode(value, CHARACTER_ENCODING)
                    .replace("+", "%20")
                    .replace("*", "%2A")
                    .replace("%7E","~");
        } catch (UnsupportedEncodingException e) {
            System.err.println("Unknown encoding: " + CHARACTER_ENCODING);
            e.printStackTrace();
        }

        return encoded;
    }



}
