from flask import Flask, send_from_directory, jsonify
from flask_cors import CORS
from backend.routes import api_bp
import os
import sqlite3

# Create Flask app
app = Flask(__name__, static_folder='frontend', static_url_path='')
CORS(app)  # Enable CORS for all routes

# Register API blueprint
app.register_blueprint(api_bp, url_prefix='/api')


# Health check route
@app.route('/api/health', methods=['GET'])
def health_check():
    try:
        # Test database connection
        conn = sqlite3.connect('hospital.db')
        conn.close()
        return jsonify({
            'status': 'healthy',
            'database': 'connected',
            'message': 'Hospital Management System API is running'
        })
    except Exception as e:
        return jsonify({
            'status': 'unhealthy',
            'database': 'disconnected',
            'error': str(e)
        }), 500


# Serve frontend
@app.route('/')
def serve_frontend():
    return send_from_directory('frontend', 'index.html')


@app.route('/<path:path>')
def serve_static(path):
    return send_from_directory('frontend', path)


# Error handlers
@app.errorhandler(404)
def not_found(error):
    return jsonify({'error': 'Resource not found'}), 404


@app.errorhandler(500)
def internal_error(error):
    return jsonify({'error': 'Internal server error'}), 500


if __name__ == '__main__':
    # Create necessary directories if they don't exist
    os.makedirs('frontend/css', exist_ok=True)
    os.makedirs('frontend/js', exist_ok=True)

    # Initialize database
    from backend.database import Database

    db = Database()

    print("=" * 60)
    print("🏥 HOSPITAL MANAGEMENT SYSTEM")
    print("=" * 60)
    print("✅ Server starting...")
    print("📍 Access the application at: http://localhost:5000")
    print("🔌 API endpoints available at: http://localhost:5000/api")
    print("📊 Database: hospital.db")
    print("=" * 60)
    print("\nAvailable API endpoints:")
    print("  GET    /api/health              - Health check")
    print("  GET    /api/patients             - List all patients")
    print("  POST   /api/patients             - Add new patient")
    print("  GET    /api/doctors              - List all doctors")
    print("  POST   /api/doctors              - Add new doctor")
    print("  GET    /api/appointments          - List all appointments")
    print("  POST   /api/appointments          - Schedule appointment")
    print("  GET    /api/stats                 - View statistics")
    print("=" * 60)

    app.run(debug=True, host='0.0.0.0', port=5000)