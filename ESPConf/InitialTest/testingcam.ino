void setup() {
  Serial.begin(115200);
  delay(2000);
  Serial.println("BOOT OK");
}

void loop() {
  delay(2000);
  Serial.println("RUNNING");
}
