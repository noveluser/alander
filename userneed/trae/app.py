from flask import Flask, render_template, request, redirect, url_for, flash
import sqlite3
import os

# 初始化Flask应用
app = Flask(__name__)
app.config['SECRET_KEY'] = 'your-secret-key'  # 用于flash消息

# 获取数据库连接
def get_db_connection():
    conn = sqlite3.connect('database.db')
    conn.row_factory = sqlite3.Row  # 使返回的结果可以像字典一样访问
    return conn

# 创建数据库表
def create_table():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            description TEXT NOT NULL
        )
    ''')
    conn.commit()
    conn.close()

# 主页路由
@app.route('/')
def index():
    conn = get_db_connection()
    items = conn.execute('SELECT * FROM items').fetchall()
    conn.close()
    return render_template('index.html', items=items)

# 添加项目路由
@app.route('/add', methods=['GET', 'POST'])
def add_item():
    if request.method == 'POST':
        name = request.form['name']
        description = request.form['description']
        
        if not name or not description:
            flash('名称和描述不能为空！')
            return redirect(url_for('add_item'))
        
        conn = get_db_connection()
        conn.execute('INSERT INTO items (name, description) VALUES (?, ?)', (name, description))
        conn.commit()
        conn.close()
        return redirect(url_for('index'))
    
    return render_template('add_item.html')

# 删除项目路由
@app.route('/delete/<int:item_id>')
def delete_item(item_id):
    conn = get_db_connection()
    conn.execute('DELETE FROM items WHERE id = ?', (item_id,))
    conn.commit()
    conn.close()
    return redirect(url_for('index'))

# 主函数，启动应用
if __name__ == '__main__':
    # 确保数据库和表存在
    if not os.path.exists('database.db'):
        create_table()
    # 启动Flask应用，设置为调试模式
    app.run(debug=True)