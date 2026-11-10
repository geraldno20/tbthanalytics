-- Run once in the Supabase SQL editor.
-- ALTER TYPE ... ADD VALUE must be run on its own (separately from other statements) if your editor wraps in a transaction.
ALTER TYPE task_status ADD VALUE IF NOT EXISTS 'na';

ALTER TABLE projects ADD COLUMN IF NOT EXISTS format TEXT NOT NULL DEFAULT 'Interview';
