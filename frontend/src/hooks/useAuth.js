// Copyright 2025 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// src/hooks/useAuth.js
import { useCallback, useState, useEffect } from 'react';
import { getAuth, signInWithPopup, GoogleAuthProvider, signOut, onAuthStateChanged } from 'firebase/auth';
import { signInAnonymousUser } from '../firebase';

export const useAuth = (setShowUserMenu) => {
  const [user, setUser] = useState(null);
  const [firstName, setFirstName] = useState('');
  const [loading, setLoading] = useState(true);
  const [isAuthenticated, setIsAuthenticated] = useState(false);
  const auth = getAuth();

  const handleLogin = useCallback(async () => {
    console.log('[AUTH_DEBUG] handleLogin called');
    const provider = new GoogleAuthProvider();
    provider.addScope('profile');
    provider.addScope('email');
    try {
      const result = await signInWithPopup(auth, provider);
      const user = result.user;
      const displayName = user?.displayName || '';
      const firstName = displayName.split(' ')[0];
      setFirstName(firstName);
      setIsAuthenticated(true);
    } catch (error) {
      console.error('[AUTH_DEBUG] Error signing in with Google');
      setIsAuthenticated(false);
    }
  }, [auth]);

  const handleLogout = useCallback(async () => {
    try {
      await signOut(auth);
      setShowUserMenu(false);
    } catch (error) {
      console.error('Error signing out');
    }
  }, [auth, setShowUserMenu]);

  // Set up auth state listener
  useEffect(() => {
    const unsubscribe = onAuthStateChanged(auth, async (firebaseUser) => {
      if (firebaseUser) {
        setUser(firebaseUser);
        const displayName = firebaseUser.displayName || '';
        const firstName = displayName.split(' ')[0];
        setFirstName(firstName);
        setIsAuthenticated(true);
      } else {
        try {
          const anonymousUser = await signInAnonymousUser();
          setUser(anonymousUser);
          setFirstName('');
          setIsAuthenticated(false);
        } catch (error) {
          console.error('[AUTH_MENU_DEBUG] useAuth: Anonymous sign in error');
          setIsAuthenticated(false);
        }
      }
      setLoading(false);
    });

    return () => {
      unsubscribe();
    };
  }, [auth]);

  return { user, firstName, loading, handleLogin, handleLogout, isAuthenticated };
};
