// Switch to fx_rates database
db = db.getSiblingDB('fx_rates');

// Create user
db.createUser({
  user: 'fx_user',
  pwd: 'fx_password',
  roles: [
    {
      role: 'readWrite',
      db: 'fx_rates'
    }
  ]
});

// Create collection with validation
db.createCollection("exchange_rates");

