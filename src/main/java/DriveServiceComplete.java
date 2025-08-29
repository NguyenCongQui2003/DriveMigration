import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.security.KeyFactory;
import java.security.PrivateKey;
import java.security.Signature;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.time.Instant;

/**
 * Service class để tương tác với Google Drive API sử dụng Service Account
 * Phiên bản đã fix các lỗi parsing và query
 */
public class DriveServiceComplete {
    private final String serviceAccountEmail;
    private final String privateKey;
    private final List<String> scopes = Arrays.asList(
            "https://www.googleapis.com/auth/drive",
            "https://www.googleapis.com/auth/drive.file"
    );

    public DriveServiceComplete(String serviceAccountEmail, String privateKey) {
        this.serviceAccountEmail = serviceAccountEmail;
        this.privateKey = privateKey;
    }

    /**
     * Tạo JWT token cho Service Account authentication
     */
    private String createJWT(String userEmail) throws Exception {
        long now = Instant.now().getEpochSecond();
        long expiry = now + 3600; // 1 hour

        // Header
        String header = "{\"alg\":\"RS256\",\"typ\":\"JWT\"}";
        String headerBase64 = Base64.getUrlEncoder().withoutPadding()
                .encodeToString(header.getBytes());

        // Payload
        String payload = String.format(
                "{\"iss\":\"%s\",\"scope\":\"%s\",\"aud\":\"https://oauth2.googleapis.com/token\",\"exp\":%d,\"iat\":%d,\"sub\":\"%s\"}",
                serviceAccountEmail,
                String.join(" ", scopes),
                expiry,
                now,
                userEmail
        );
        String payloadBase64 = Base64.getUrlEncoder().withoutPadding()
                .encodeToString(payload.getBytes());

        // Signature
        String signatureInput = headerBase64 + "." + payloadBase64;
        String signature = signWithPrivateKey(signatureInput, privateKey);

        return signatureInput + "." + signature;
    }

    /**
     * Ký JWT với private key
     */
    private String signWithPrivateKey(String data, String privateKeyPem) throws Exception {
        // Clean private key
        String cleanKey = privateKeyPem
                .replace("-----BEGIN PRIVATE KEY-----", "")
                .replace("-----END PRIVATE KEY-----", "")
                .replaceAll("\\s", "");

        byte[] keyBytes = Base64.getDecoder().decode(cleanKey);
        PKCS8EncodedKeySpec keySpec = new PKCS8EncodedKeySpec(keyBytes);
        KeyFactory keyFactory = KeyFactory.getInstance("RSA");
        PrivateKey key = keyFactory.generatePrivate(keySpec);

        Signature signature = Signature.getInstance("SHA256withRSA");
        signature.initSign(key);
        signature.update(data.getBytes());

        byte[] signatureBytes = signature.sign();
        return Base64.getUrlEncoder().withoutPadding().encodeToString(signatureBytes);
    }

    /**
     * Lấy access token từ JWT
     */
    private String getAccessToken(String userEmail) throws Exception {
        String jwt = createJWT(userEmail);

        URL url = new URL("https://oauth2.googleapis.com/token");
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("POST");
        conn.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
        conn.setDoOutput(true);

        String postData = "grant_type=urn:ietf:params:oauth:grant-type:jwt-bearer&assertion=" + jwt;

        try (OutputStream os = conn.getOutputStream()) {
            os.write(postData.getBytes());
        }

        int responseCode = conn.getResponseCode();
        if (responseCode != 200) {
            try (BufferedReader errorReader = new BufferedReader(new InputStreamReader(conn.getErrorStream()))) {
                StringBuilder errorResponse = new StringBuilder();
                String line;
                while ((line = errorReader.readLine()) != null) {
                    errorResponse.append(line);
                }
                throw new RuntimeException("Failed to get access token: " + responseCode + " - " + errorResponse.toString());
            }
        }

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
            StringBuilder response = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                response.append(line);
            }

            // Parse JSON response to get access_token
            String jsonResponse = response.toString();
            Pattern pattern = Pattern.compile("\"access_token\"\\s*:\\s*\"([^\"]+)\"");
            Matcher matcher = pattern.matcher(jsonResponse);

            if (matcher.find()) {
                return matcher.group(1);
            } else {
                throw new RuntimeException("Could not extract access token from response: " + jsonResponse);
            }
        }
    }

    /**
     * Thực hiện HTTP request đến Google Drive API
     */
    private String makeApiRequest(String endpoint, String method, String payload, String userEmail) throws Exception {
        String accessToken = getAccessToken(userEmail);

        URL url = new URL(endpoint);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod(method);
        conn.setRequestProperty("Authorization", "Bearer " + accessToken);
        conn.setRequestProperty("Content-Type", "application/json");

        if (payload != null && !payload.isEmpty()) {
            conn.setDoOutput(true);
            try (OutputStream os = conn.getOutputStream()) {
                os.write(payload.getBytes("UTF-8"));
            }
        }

        int responseCode = conn.getResponseCode();

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(
                responseCode >= 200 && responseCode < 300 ? conn.getInputStream() : conn.getErrorStream(), "UTF-8"))) {
            StringBuilder response = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                response.append(line);
            }

            if (responseCode >= 200 && responseCode < 300) {
                return response.toString();
            } else {
                throw new RuntimeException("API request failed: " + responseCode + " - " + response.toString());
            }
        }
    }

    /**
     * Lấy tất cả files của user - VERSION CẢI TIẾN
     */
    public List<DriveFile> getAllFiles(String userEmail) throws Exception {
        List<DriveFile> files = new ArrayList<>();
        String pageToken = null;
        int totalRetrieved = 0;

        System.out.println("DEBUG: Starting getAllFiles for user: " + userEmail);

        do {
            // CẢI TIẾN: Thêm q parameter để loại trừ trash và chỉ lấy files có shared
            String endpoint = "https://www.googleapis.com/drive/v3/files" +
                    "?pageSize=100" + // Giảm page size để tránh timeout
                    "&q=trashed=false" + // Loại trừ files trong trash
                    "&fields=nextPageToken,files(id,name,mimeType,owners,permissions(role,emailAddress,type),capabilities)" +
                    (pageToken != null ? "&pageToken=" + pageToken : "");

            System.out.println("DEBUG: Making API request to: " + endpoint);

            String response = makeApiRequest(endpoint, "GET", null, userEmail);

            System.out.println("DEBUG: API Response length: " + response.length());
            System.out.println("DEBUG: First 500 chars of response: " +
                    (response.length() > 500 ? response.substring(0, 500) + "..." : response));

            // Parse response
            List<DriveFile> pageFiles = parseFilesFromResponse(response);
            System.out.println("DEBUG: Parsed " + pageFiles.size() + " files from this page");

            files.addAll(pageFiles);
            totalRetrieved += pageFiles.size();

            // Get next page token
            pageToken = extractNextPageToken(response);
            System.out.println("DEBUG: Next page token: " + (pageToken != null ? "exists" : "null"));

            // Rate limiting
            Thread.sleep(200);

        } while (pageToken != null && totalRetrieved < 10000); // Safety limit

        System.out.println("DEBUG: Total files retrieved for " + userEmail + ": " + files.size());
        return files;
    }

    /**
     * Xử lý Drive của một user
     */
    public MigrationResult processUserDrive(String userEmail, Map<String, String> userMapping,
                                            FileProgressCallback callback) throws Exception {
        MigrationResult result = new MigrationResult();
        result.userEmail = userEmail;
        result.startTime = new Date();

        try {
            System.out.println("DEBUG: Starting processUserDrive for: " + userEmail);
            System.out.println("DEBUG: User mapping contains " + userMapping.size() + " entries");

            // Get all files
            System.out.println("DEBUG: Getting all files for user: " + userEmail);
            List<DriveFile> files = getAllFiles(userEmail);
            result.totalFiles = files.size();

            System.out.println("DEBUG: Found " + files.size() + " files for user: " + userEmail);

            if (files.isEmpty()) {
                System.out.println("DEBUG: No files found for user: " + userEmail);
                result.endTime = new Date();
                result.success = true;
                return result;
            }

            // Process each file
            for (int i = 0; i < files.size(); i++) {
                DriveFile file = files.get(i);

                System.out.println("DEBUG: Processing file " + (i+1) + "/" + files.size() + ": " + file.name);

                FileProcessingResult fileResult = processFilePermissions(file, userMapping, userEmail);
                result.fileResults.add(fileResult);

                // Update counters
                switch (fileResult.status) {
                    case "SUCCESS":
                        result.successFiles++;
                        break;
                    case "ERROR":
                        result.failedFiles++;
                        break;
                    case "RESTRICTED":
                        result.restrictedFiles++;
                        break;
                    case "SKIPPED":
                        result.skippedFiles++;
                        break;
                }

                if (callback != null) {
                    callback.onFileProcessed(userEmail, i + 1, files.size(), fileResult);
                }

                // Rate limiting
                if (i % 5 == 0 && i > 0) {
                    Thread.sleep(1000);
                }
            }

            result.endTime = new Date();
            result.success = true;

        } catch (Exception e) {
            result.endTime = new Date();
            result.success = false;
            result.errorMessage = e.getMessage();
            System.out.println("DEBUG: Exception in processUserDrive for " + userEmail + ": " + e.getMessage());
            e.printStackTrace();
            throw e;
        }

        return result;
    }

    /**
     * Xử lý quyền của một file
     */
    private FileProcessingResult processFilePermissions(DriveFile file, Map<String, String> userMapping, String userEmail) {
        FileProcessingResult result = new FileProcessingResult();
        result.fileName = file.name;
        result.fileId = file.id;
        result.fileType = getFileType(file.mimeType);

        try {
            System.out.println("DEBUG: Processing file: " + file.name + " (ID: " + file.id + ")");

            // Kiểm tra xem file có thể chia sẻ không
            if (!canShareFile(file)) {
                result.status = "RESTRICTED";
                result.errorMessage = "File sharing is restricted";
                return result;
            }

            if (file.permissions == null || file.permissions.isEmpty()) {
                result.status = "SKIPPED";
                result.errorMessage = "No permissions to process";
                return result;
            }

            System.out.println("DEBUG: File " + file.name + " has " + file.permissions.size() + " permissions");

            boolean hasSuccessfulShare = false;
            boolean hasAnyPermissionToProcess = false;

            // Xử lý từng permission
            for (DrivePermission permission : file.permissions) {
                String oldEmail = permission.emailAddress;

                System.out.println("DEBUG: Processing permission - Email: " + oldEmail + ", Role: " + permission.role + ", Type: " + permission.type);

                // Skip owner permissions
                if ("owner".equals(permission.role)) {
                    System.out.println("DEBUG: Skipping owner permission");
                    continue;
                }

                // Skip domain permissions
                if ("domain".equals(permission.type)) {
                    System.out.println("DEBUG: Skipping domain permission");
                    continue;
                }

                // Skip anyone permissions
                if ("anyone".equals(permission.type)) {
                    System.out.println("DEBUG: Skipping anyone permission");
                    continue;
                }

                // Only process user type permissions with email
                if (!"user".equals(permission.type) || oldEmail == null || oldEmail.trim().isEmpty()) {
                    System.out.println("DEBUG: Skipping non-user permission or empty email");
                    continue;
                }

                // Check if we have mapping for this email
                if (userMapping.containsKey(oldEmail)) {
                    hasAnyPermissionToProcess = true;
                    String newEmail = userMapping.get(oldEmail);

                    System.out.println("DEBUG: Found mapping: " + oldEmail + " -> " + newEmail);

                    result.oldEmail = oldEmail;
                    result.newEmail = newEmail;
                    result.role = permission.role;
                    result.permissionType = getPermissionType(permission.role);

                    try {
                        // Create new permission
                        String permissionPayload = String.format(
                                "{\"type\":\"user\",\"role\":\"%s\",\"emailAddress\":\"%s\",\"sendNotificationEmails\":false,\"suppressNotifications\":true,\"sendNotificationEmail\":false}",
                                permission.role, newEmail
                        );

                        String endpoint = String.format(
                                "https://www.googleapis.com/drive/v3/files/%s/permissions",
                                file.id
                        );

                        System.out.println("DEBUG: Adding permission for " + newEmail + " with role " + permission.role);

                        String response = makeApiRequest(endpoint, "POST", permissionPayload, userEmail);

                        hasSuccessfulShare = true;
                        result.status = "SUCCESS";
                        result.permissionsAdded++;

                        System.out.println("DEBUG: Successfully added permission for " + newEmail);

                        // Rate limiting
                        Thread.sleep(300);

                    } catch (Exception e) {
                        String errorMessage = e.getMessage();
                        System.out.println("DEBUG: Error adding permission: " + errorMessage);

                        if (errorMessage != null && (errorMessage.contains("restricted") ||
                                errorMessage.contains("flagged") ||
                                errorMessage.contains("sharingNotAllowed"))) {
                            result.status = "RESTRICTED";
                            result.errorMessage = errorMessage;
                        } else {
                            result.status = "ERROR";
                            result.errorMessage = errorMessage;
                        }
                    }
                } else {
                    System.out.println("DEBUG: No mapping found for email: " + oldEmail);
                }
            }

            // Xác định kết quả cuối cùng
            if (!hasAnyPermissionToProcess) {
                result.status = "SKIPPED";
                result.errorMessage = "No permissions to migrate";
            } else if (!hasSuccessfulShare && !"RESTRICTED".equals(result.status)) {
                result.status = "ERROR";
                if (result.errorMessage == null) {
                    result.errorMessage = "Failed to add any permissions";
                }
            }

        } catch (Exception e) {
            result.status = "ERROR";
            result.errorMessage = e.getMessage();
            System.out.println("DEBUG: Exception processing file " + file.name + ": " + e.getMessage());
            e.printStackTrace();
        }

        return result;
    }

    /**
     * Kiểm tra xem file có thể chia sẻ không
     */
    private boolean canShareFile(DriveFile file) {
        // Có thể thêm logic kiểm tra capabilities.canShare ở đây
        return true;
    }

    /**
     * Test connection với một user
     */
    public boolean testConnection(String userEmail) {
        try {
            System.out.println("DEBUG: Testing connection for user: " + userEmail);
            String endpoint = "https://www.googleapis.com/drive/v3/about?fields=user";
            String response = makeApiRequest(endpoint, "GET", null, userEmail);
            System.out.println("DEBUG: Test connection successful for: " + userEmail);
            return true;
        } catch (Exception e) {
            System.out.println("DEBUG: Test connection failed for: " + userEmail);
            System.out.println("DEBUG: Error: " + e.getMessage());
            e.printStackTrace();
            return false;
        }
    }

    /**
     * Lấy thông tin chi tiết về user
     */
    public String getUserInfo(String userEmail) {
        try {
            String endpoint = "https://www.googleapis.com/drive/v3/about?fields=user,storageQuota";
            String response = makeApiRequest(endpoint, "GET", null, userEmail);
            return response;
        } catch (Exception e) {
            return "Error getting user info: " + e.getMessage();
        }
    }

    /**
     * Xác định loại file
     */
    private String getFileType(String mimeType) {
        if ("application/vnd.google-apps.folder".equals(mimeType)) {
            return "Folder";
        } else {
            return "File";
        }
    }

    /**
     * Xác định loại permission
     */
    private String getPermissionType(String role) {
        switch (role) {
            case "reader":
            case "viewer":
                return "View";
            case "writer":
                return "Edit";
            case "commenter":
                return "Comment";
            case "owner":
                return "Owner";
            default:
                return "Unknown";
        }
    }

    /**
     * Parse files từ JSON response - VERSION CẢI TIẾN
     */
    private List<DriveFile> parseFilesFromResponse(String jsonResponse) {
        List<DriveFile> files = new ArrayList<>();

        try {
            System.out.println("DEBUG: Starting to parse files from response");

            // Tìm files array
            int filesIndex = jsonResponse.indexOf("\"files\"");
            if (filesIndex == -1) {
                System.out.println("DEBUG: No 'files' key found in response");
                return files;
            }

            // Tìm array start
            int arrayStart = jsonResponse.indexOf("[", filesIndex);
            if (arrayStart == -1) {
                System.out.println("DEBUG: No array found after 'files' key");
                return files;
            }

            // Tìm array end bằng cách đếm brackets
            int arrayEnd = findMatchingBracket(jsonResponse, arrayStart);
            if (arrayEnd == -1) {
                System.out.println("DEBUG: Could not find matching bracket for files array");
                return files;
            }

            String filesArrayContent = jsonResponse.substring(arrayStart + 1, arrayEnd);
            System.out.println("DEBUG: Files array content length: " + filesArrayContent.length());

            if (filesArrayContent.trim().isEmpty()) {
                System.out.println("DEBUG: Files array is empty");
                return files;
            }

            // Parse từng file object
            List<String> fileObjects = extractJsonObjects(filesArrayContent);
            System.out.println("DEBUG: Found " + fileObjects.size() + " file objects");

            for (String fileJson : fileObjects) {
                DriveFile file = parseFileFromJson(fileJson);
                if (file != null) {
                    files.add(file);
                }
            }

            System.out.println("DEBUG: Successfully parsed " + files.size() + " files");

        } catch (Exception e) {
            System.err.println("ERROR: Error parsing files response: " + e.getMessage());
            e.printStackTrace();
        }

        return files;
    }

    /**
     * Tìm matching bracket
     */
    private int findMatchingBracket(String json, int startIndex) {
        int count = 0;
        for (int i = startIndex; i < json.length(); i++) {
            char c = json.charAt(i);
            if (c == '[') {
                count++;
            } else if (c == ']') {
                count--;
                if (count == 0) {
                    return i;
                }
            }
        }
        return -1;
    }

    /**
     * Extract JSON objects từ array content
     */
    private List<String> extractJsonObjects(String arrayContent) {
        List<String> objects = new ArrayList<>();

        int start = 0;
        int braceCount = 0;
        boolean inString = false;
        boolean escaped = false;

        for (int i = 0; i < arrayContent.length(); i++) {
            char c = arrayContent.charAt(i);

            if (escaped) {
                escaped = false;
                continue;
            }

            if (c == '\\') {
                escaped = true;
                continue;
            }

            if (c == '"') {
                inString = !inString;
                continue;
            }

            if (inString) {
                continue;
            }

            if (c == '{') {
                if (braceCount == 0) {
                    start = i;
                }
                braceCount++;
            } else if (c == '}') {
                braceCount--;
                if (braceCount == 0) {
                    String obj = arrayContent.substring(start, i + 1);
                    objects.add(obj);
                }
            }
        }

        return objects;
    }

    /**
     * Parse một file object từ JSON - VERSION CẢI TIẾN
     */
    private DriveFile parseFileFromJson(String fileJson) {
        try {
            DriveFile file = new DriveFile();

            // Extract id
            file.id = extractJsonValue(fileJson, "id");
            if (file.id == null) {
                System.out.println("DEBUG: File missing ID, skipping");
                return null;
            }

            // Extract name
            file.name = extractJsonValue(fileJson, "name");
            if (file.name == null) {
                file.name = "Unnamed File";
            }

            // Extract mimeType
            file.mimeType = extractJsonValue(fileJson, "mimeType");

            // Extract permissions
            file.permissions = parsePermissionsFromJson(fileJson);

            System.out.println("DEBUG: Parsed file: " + file.name + " with " + file.permissions.size() + " permissions");

            return file;
        } catch (Exception e) {
            System.err.println("ERROR: Error parsing file JSON: " + e.getMessage());
            return null;
        }
    }

    /**
     * Extract JSON value
     */
    private String extractJsonValue(String json, String key) {
        try {
            Pattern pattern = Pattern.compile("\"" + key + "\"\\s*:\\s*\"([^\"\\\\]*(\\\\.[^\"\\\\]*)*)\"");
            Matcher matcher = pattern.matcher(json);
            if (matcher.find()) {
                return matcher.group(1).replace("\\\"", "\"").replace("\\\\", "\\");
            }
        } catch (Exception e) {
            System.err.println("Error extracting " + key + ": " + e.getMessage());
        }
        return null;
    }

    /**
     * Parse permissions từ file JSON - VERSION CẢI TIẾN
     */
    private List<DrivePermission> parsePermissionsFromJson(String fileJson) {
        List<DrivePermission> permissions = new ArrayList<>();

        try {
            // Tìm permissions array
            int permissionsIndex = fileJson.indexOf("\"permissions\"");
            if (permissionsIndex == -1) {
                return permissions;
            }

            int arrayStart = fileJson.indexOf("[", permissionsIndex);
            if (arrayStart == -1) {
                return permissions;
            }

            int arrayEnd = findMatchingBracket(fileJson, arrayStart);
            if (arrayEnd == -1) {
                return permissions;
            }

            String permissionsContent = fileJson.substring(arrayStart + 1, arrayEnd);

            if (permissionsContent.trim().isEmpty()) {
                return permissions;
            }

            // Parse từng permission object
            List<String> permissionObjects = extractJsonObjects(permissionsContent);

            for (String permissionJson : permissionObjects) {
                DrivePermission permission = new DrivePermission();

                permission.role = extractJsonValue(permissionJson, "role");
                permission.emailAddress = extractJsonValue(permissionJson, "emailAddress");
                permission.type = extractJsonValue(permissionJson, "type");

                if (permission.role != null) {
                    permissions.add(permission);
                }
            }

        } catch (Exception e) {
            System.err.println("ERROR: Error parsing permissions: " + e.getMessage());
        }

        return permissions;
    }

    /**
     * Extract next page token từ response
     */
    private String extractNextPageToken(String jsonResponse) {
        return extractJsonValue(jsonResponse, "nextPageToken");
    }
}

/**
 * Interface cho callback trong quá trình xử lý file
 */
interface FileProgressCallback {
    void onFileProcessed(String userEmail, int currentFile, int totalFiles, FileProcessingResult result);
}

/**
 * Class đại diện cho một file trong Drive
 */
class DriveFile {
    public String id;
    public String name;
    public String mimeType;
    public List<DrivePermission> permissions = new ArrayList<>();
}

/**
 * Class đại diện cho một permission
 */
class DrivePermission {
    public String role;
    public String emailAddress;
    public String type;
}

/**
 * Kết quả xử lý một file
 */
class FileProcessingResult {
    public String fileName;
    public String fileId;
    public String fileType;
    public String permissionType;
    public String status;
    public String oldEmail;
    public String newEmail;
    public String role;
    public String errorMessage;
    public int permissionsAdded = 0;
}

/**
 * Kết quả migration của một user
 */
class MigrationResult {
    public String userEmail;
    public Date startTime;
    public Date endTime;
    public boolean success;
    public String errorMessage;
    public int totalFiles;
    public int successFiles;
    public int failedFiles;
    public int restrictedFiles;
    public int skippedFiles;
    public List<FileProcessingResult> fileResults = new ArrayList<>();
}

/**
 * Statistics cho migration
 */
class MigrationStats {
    public int totalFiles;
    public int successFiles;
    public int failedFiles;
    public int restrictedFiles;
    public int skippedFiles;
}