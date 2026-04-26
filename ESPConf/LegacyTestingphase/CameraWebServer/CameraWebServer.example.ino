#include <Arduino.h>
#include "esp_camera.h"
#include <WiFi.h>

// ===========================
// Select camera model in board_config.h
// ===========================
#include "board_config.h"

// ===========================
// Enter your WiFi credentials
// ===========================
// Keep these values real for your own deployment.
// For public templates, replace with placeholders.
const char *ssid = "YOUR_WIFI_SSID_HERE";
const char *password = "YOUR_WIFI_PASSWORD_HERE";

// ===========================
// Shared control token
// ===========================
// app_httpd.cpp can use this token to protect control endpoints.
// Backend must send the same token when calling protected routes.
const char kControlApiToken[] = "YOUR_CAMERA_CONTROL_TOKEN_HERE";

// ===========================
// Stable boot profile (firmware baseline)
// ===========================
// Backend can override after reconnect depending on active mode.
constexpr uint32_t kWiFiConnectTimeoutMs = 120000;  // 2 minutes
constexpr uint32_t kWiFiRetryDelayMs = 500;

constexpr int kBootXclkHz = 15000000;  // 15 MHz baseline
constexpr framesize_t kBootFrameSize = FRAMESIZE_HVGA;
constexpr int kBootJpegQuality = 4;
constexpr int kBootFrameBufferCount = 2;
constexpr camera_grab_mode_t kBootGrabMode = CAMERA_GRAB_LATEST;

void startCameraServer();
void setupLedFlash();

static void restartWithMessage(const char *msg, uint32_t delayMs = 5000) {
  Serial.println();
  Serial.println(msg);
  Serial.printf("Restarting in %lu ms...\n", static_cast<unsigned long>(delayMs));
  delay(delayMs);
  ESP.restart();
}

static void applyBootSensorProfile(sensor_t *s) {
  if (!s) {
    return;
  }

  // Core stream profile
  s->set_framesize(s, kBootFrameSize);
  s->set_quality(s, kBootJpegQuality);

  // Neutral image controls (backend/adaptive can tune later)
  s->set_brightness(s, 0);
  s->set_contrast(s, 0);
  s->set_saturation(s, 0);

  // Auto controls and baseline toggles
  s->set_whitebal(s, 1);      // AWB enabled
  s->set_awb_gain(s, 1);      // AWB gain enabled
  s->set_wb_mode(s, 0);       // Auto WB mode
  s->set_gain_ctrl(s, 1);     // AGC enabled
  s->set_exposure_ctrl(s, 1); // AEC sensor enabled
  s->set_aec2(s, 0);          // AEC DSP disabled
  s->set_special_effect(s, 0);

  // Requested baseline processing toggles
  s->set_bpc(s, 0);
  s->set_wpc(s, 1);
  s->set_raw_gma(s, 1);
  s->set_lenc(s, 1);
  s->set_dcw(s, 1);
  s->set_colorbar(s, 0);

  // Orientation baseline
  s->set_hmirror(s, 0);
  s->set_vflip(s, 0);
}

void setup() {
  Serial.begin(115200);
  Serial.setDebugOutput(true);
  Serial.println();
  Serial.println("Booting camera firmware...");

  camera_config_t config = {};
  config.ledc_channel = LEDC_CHANNEL_0;
  config.ledc_timer = LEDC_TIMER_0;

  config.pin_d0 = Y2_GPIO_NUM;
  config.pin_d1 = Y3_GPIO_NUM;
  config.pin_d2 = Y4_GPIO_NUM;
  config.pin_d3 = Y5_GPIO_NUM;
  config.pin_d4 = Y6_GPIO_NUM;
  config.pin_d5 = Y7_GPIO_NUM;
  config.pin_d6 = Y8_GPIO_NUM;
  config.pin_d7 = Y9_GPIO_NUM;

  config.pin_xclk = XCLK_GPIO_NUM;
  config.pin_pclk = PCLK_GPIO_NUM;
  config.pin_vsync = VSYNC_GPIO_NUM;
  config.pin_href = HREF_GPIO_NUM;
  config.pin_sccb_sda = SIOD_GPIO_NUM;
  config.pin_sccb_scl = SIOC_GPIO_NUM;
  config.pin_pwdn = PWDN_GPIO_NUM;
  config.pin_reset = RESET_GPIO_NUM;

  config.xclk_freq_hz = kBootXclkHz;
  config.pixel_format = PIXFORMAT_JPEG;
  config.frame_size = kBootFrameSize;
  config.jpeg_quality = kBootJpegQuality;
  config.fb_count = kBootFrameBufferCount;
  config.grab_mode = kBootGrabMode;
  config.fb_location = CAMERA_FB_IN_PSRAM;

  esp_err_t err = esp_camera_init(&config);
  if (err != ESP_OK) {
    Serial.printf("Camera init failed with error 0x%x\n", err);
    restartWithMessage("Camera init failure.");
    return;
  }

  sensor_t *s = esp_camera_sensor_get();
  if (!s) {
    restartWithMessage("Camera sensor handle is null.");
    return;
  }

  applyBootSensorProfile(s);

#if defined(LED_GPIO_NUM)
  // LED is initialized to 0 intensity baseline in app_httpd.cpp.
  setupLedFlash();
#endif

  WiFi.mode(WIFI_STA);
  WiFi.setSleep(false);
  WiFi.begin(ssid, password);

  Serial.print("WiFi connecting");
  uint32_t startMs = millis();
  while (WiFi.status() != WL_CONNECTED) {
    if (millis() - startMs > kWiFiConnectTimeoutMs) {
      restartWithMessage("WiFi connection timeout.");
      return;
    }
    delay(kWiFiRetryDelayMs);
    Serial.print(".");
  }

  Serial.println();
  Serial.println("WiFi connected");

  startCameraServer();

  Serial.print("Camera settings UI: http://");
  Serial.println(WiFi.localIP());

  Serial.print("Camera stream URL: http://");
  Serial.print(WiFi.localIP());
  Serial.println(":81/stream");
}

void loop() {
  // Web server tasks handle camera operations.
  delay(10000);
}
