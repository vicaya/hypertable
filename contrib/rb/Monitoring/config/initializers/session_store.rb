# Be sure to restart your server when you modify this file.

# Your secret key for verifying cookie session data integrity.
# If you change this key, all old sessions will become invalid!
# Make sure the secret is at least 30 characters and all random, 
# no regular words or you'll be exposed to dictionary attacks.
ActionController::Base.session = {
  :key         => '_hyptertable_metrics_session',
  :secret      => '33a8cf5f27d225da9a4f71465f579257ab865b795cb054ed0e0cac0cc5560c01cd84b18342208b67477c54923f7526bd9d74beb94c1f6271c67d5ffae0d26f55'
}

# Use the database for sessions instead of the cookie-based default,
# which shouldn't be used to store highly confidential information
# (create the session table with "rake db:sessions:create")
# ActionController::Base.session_store = :active_record_store
