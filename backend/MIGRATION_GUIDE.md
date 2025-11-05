# Database Migration Guide

## Automatic Migration (Recommended)

Starting from commit `8244f2e`, DCDock includes automatic database migration support.

### For Existing Databases

If you're seeing the error:
```
sqlite3.OperationalError: no such column: ramps.direction
```

**Solution**: Simply restart your backend server. Migrations will run automatically on startup.

```bash
# Stop the backend (Ctrl+C)
# Then restart:
./start_backend.sh
```

The migration script will:
1. ✅ Check if the `direction` column exists
2. ✅ Add the column if missing
3. ✅ Update existing ramps with appropriate directions:
   - R1-R4 → Inbound (IB)
   - R5+ → Outbound (OB)

### Verification

After restart, you should see in the logs:
```
Running database migrations...
Checking if 'direction' column exists in 'ramps' table...
✓ Added 'direction' column
✓ Updated existing ramps with direction
✓ Migration completed successfully
```

---

## Manual Migration (If Needed)

If automatic migration fails, you can manually update your database:

### Option 1: Fresh Database (Loses Data)

```bash
# Backup current database (optional)
cp backend/dcdock.db backend/dcdock.db.backup

# Remove old database
rm backend/dcdock.db

# Restart backend - will create new database with seed data
./start_backend.sh
```

### Option 2: SQL Migration (Preserves Data)

```bash
# Open SQLite shell
sqlite3 backend/dcdock.db

# Run migration SQL
ALTER TABLE ramps ADD COLUMN direction VARCHAR(2);

UPDATE ramps
SET direction = CASE
    WHEN CAST(SUBSTR(code, 2) AS INTEGER) <= 4 THEN 'IB'
    ELSE 'OB'
END
WHERE code LIKE 'R%';

UPDATE ramps SET direction = 'IB' WHERE direction IS NULL;

.quit
```

Then restart the backend.

---

## Migration Details

### What Changed

**Commit**: `8244f2e` - Add direction (IB/OB) field to Ramp model

**Changes**:
- Added `direction` field to `Ramp` model (LoadDirection enum: IB/OB)
- Each dock is now permanently assigned to either Inbound or Outbound
- No more shared docks

**Benefits**:
- Clear operational separation
- Prevents confusion
- Better workflow management

### Default Direction Assignment

For existing ramps with codes following the pattern `R1`, `R2`, etc.:
- **R1-R4** → Inbound (IB)
- **R5 and above** → Outbound (OB)

For custom ramp codes, the default is Inbound (IB).

---

## Troubleshooting

### Migration Fails

If you see errors during migration:

1. **Check database permissions**:
   ```bash
   ls -la backend/dcdock.db
   # Should be writable by your user
   ```

2. **Check database integrity**:
   ```bash
   sqlite3 backend/dcdock.db "PRAGMA integrity_check;"
   ```

3. **Try manual migration** (see Option 2 above)

### Column Already Exists

If migration says "column already exists" but you still get errors:

```bash
# Verify column exists
sqlite3 backend/dcdock.db "PRAGMA table_info(ramps);"

# Check for NULL values
sqlite3 backend/dcdock.db "SELECT code, direction FROM ramps WHERE direction IS NULL;"
```

### Need Help?

Create an issue at: https://github.com/TMMCx2/DCDock/issues
