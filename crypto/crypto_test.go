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

package crypto

import (
	"strings"
	"testing"
)

func Test_Crypt(t *testing.T) {

	// TODO: Test for pw too short, etc.

	plain := "Hello there this is a test\n"
	passwd := "foobarbaz weijr woeijfoisjd foij"

	for i := 1; i < 100; i++ {

		s1 := strings.Repeat(plain, i)

		cipher, err := EncryptString(s1, passwd)
		if err != nil {
			t.Errorf("Error: %v", err)
			break
		}

		s2, err := DecryptString(cipher, passwd)
		if err != nil {
			t.Errorf("Error: %v", err)
			break
		}

		if s1 != s2 {
			t.Errorf("Encrypt/Decrypt failed at i %d", i)
			break
		}
	}

}
