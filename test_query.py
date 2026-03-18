import sqlite3

def run():
    conn = sqlite3.connect('hr.db')
    c = conn.cursor()
    c.execute("SELECT id, name, dept, job_title, employee_code FROM employees WHERE dept LIKE '%القوة%' LIMIT 20")
    with open('deps.txt', 'w', encoding='utf-8') as f:
        for row in c.fetchall():
            f.write(str(row) + '\n')
    conn.close()

if __name__ == '__main__':
    run()
