/* Copyright 2019 Vox Media, Inc.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       https://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License. */

// Basic encrypt/decrypt functionality using AES-GCM. See
// https://golang.org/pkg/crypto/cipher for more detail.
package crypto

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"io"
)

const keyPad = "l@te f33 app13z @f+r"

func EncryptString(text, secret string) (string, error) {
	if len(secret) < 8 {
		return "", fmt.Errorf("secret too short")
	}
	key := []byte(padRight(secret, keyPad, 32)[:32])
	plaintext := []byte(text)

	block, err := aes.NewCipher(key)
	if err != nil {
		return "", err
	}

	aesgcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}

	nsize := aesgcm.NonceSize()
	ciphertext := make([]byte, nsize, nsize+len(plaintext))
	nonce := ciphertext[:nsize]
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return "", err
	}
	ciphertext = aesgcm.Seal(ciphertext, nonce, plaintext, nil)

	return base64.URLEncoding.EncodeToString(ciphertext), nil
}

func DecryptString(text, secret string) (string, error) {
	key := []byte(padRight(secret, keyPad, 32)[:32])
	ciphertext, err := base64.URLEncoding.DecodeString(text)
	if err != nil {
		return "", err
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return "", err
	}

	aesgcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}

	nsize := aesgcm.NonceSize()
	if len(ciphertext) < nsize {
		return "", fmt.Errorf("Ciphertext too short: %d", len(ciphertext))
	}
	nonce := ciphertext[:nsize]
	ciphertext = ciphertext[nsize:]

	plaintext, err := aesgcm.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("%s", plaintext), nil
}

func padRight(s, p string, l int) string {
	for {
		s += p
		if len(s) > l {
			return s[0:l]
		}
	}
}

// Generates a 32-byte secure random token.
func GenerateToken() ([]byte, error) {
	token := make([]byte, 32)
	n, err := io.ReadFull(rand.Reader, token)
	if n != len(token) || err != nil {
		return nil, err
	}
	return token, nil
}
