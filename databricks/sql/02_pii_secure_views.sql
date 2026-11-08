-- Dynamic view: auditors see decrypted PII, everyone else sees masked values.
--
-- Silver stores PII as  v1:<base64(nonce(12) || ciphertext || tag(16))>  produced by AES-256-GCM
-- with the column name as associated data. That is exactly the layout Spark's built-in
-- aes_decrypt(input, key, 'GCM', 'DEFAULT', aad) expects, so decryption happens inside SQL with
-- the key read from a Databricks secret scope - the key never appears in code or query text.
--
-- Prerequisite (once):
--   databricks secrets create-scope fintxn
--   databricks secrets put-secret fintxn encryption_key   # base64 32-byte key, same as .env

CREATE OR REPLACE VIEW workspace.fintxn_silver.customers_secure AS
SELECT
  customer_id,
  CASE WHEN is_account_group_member('auditors')
       THEN CAST(aes_decrypt(unbase64(substr(first_name_enc, 4)),
                             unbase64(secret('fintxn', 'encryption_key')), 'GCM', 'DEFAULT',
                             'first_name') AS STRING)
       ELSE NULL END AS first_name,
  CASE WHEN is_account_group_member('auditors')
       THEN CAST(aes_decrypt(unbase64(substr(last_name_enc, 4)),
                             unbase64(secret('fintxn', 'encryption_key')), 'GCM', 'DEFAULT',
                             'last_name') AS STRING)
       ELSE NULL END AS last_name,
  CASE WHEN is_account_group_member('auditors')
       THEN CAST(aes_decrypt(unbase64(substr(email_enc, 4)),
                             unbase64(secret('fintxn', 'encryption_key')), 'GCM', 'DEFAULT',
                             'email') AS STRING)
       ELSE email_masked END AS email,
  CASE WHEN is_account_group_member('auditors')
       THEN CAST(aes_decrypt(unbase64(substr(phone_enc, 4)),
                             unbase64(secret('fintxn', 'encryption_key')), 'GCM', 'DEFAULT',
                             'phone') AS STRING)
       ELSE phone_masked END AS phone,
  birth_year,
  home_city,
  home_country,
  segment,
  signup_date
FROM workspace.fintxn_silver.customers;

-- Card tokens are already irreversible (HMAC); expose last4 only, never the token, to analysts.
CREATE OR REPLACE VIEW workspace.fintxn_gold.cards_analyst AS
SELECT card_id, customer_id, card_last4, network, card_type, status
FROM workspace.fintxn_silver.cards;
